import json
import asyncio
import httpx
import datetime
from typing import Optional, Dict, List
from django.conf import settings
from django.utils.cache import caches
from django.utils import timezone
from django.db import connections
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from dataclasses import dataclass
from urllib.parse import urljoin

@dataclass
class LeafAPIConfig:
    """Configuration for LEAF API with validation"""
    host: str
    port: str
    client_id: str
    client_secret: str
    timeout: int
    max_connections: int
    max_keepalive_connections: int
    cache_ttl: int
    token_cache_key: str
    content_cache_prefix: str
    
    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @classmethod
    def from_settings(cls) -> 'LeafAPIConfig':
        """Create config from Django settings"""
        config = settings.LEAF_API_CONFIG
        return cls(
            host=config['HOST'],
            port=config['PORT'],
            client_id=config['CLIENT_ID'],
            client_secret=config['CLIENT_SECRET'],
            timeout=config['TIMEOUT'],
            max_connections=config['MAX_CONNECTIONS'],
            max_keepalive_connections=config['MAX_KEEPALIVE_CONNECTIONS'],
            cache_ttl=config['CACHE_TTL'],
            token_cache_key=config['TOKEN_CACHE_KEY'],
            content_cache_prefix=config['CONTENT_CACHE_PREFIX']
        )

class LeafAPIClient:
    """Dedicated API client for LEAF services"""
    def __init__(self, config: LeafAPIConfig):
        self.config = config
        self._client = httpx.AsyncClient(
            timeout=config.timeout,
            limits=httpx.Limits(
                max_keepalive_connections=config.max_keepalive_connections,
                max_connections=config.max_connections
            )
        )
        self._cache = caches['default']

    async def close(self):
        await self._client.aclose()

    @database_sync_to_async
    def _get_cached_token(self) -> Optional[str]:
        """Get token from cache"""
        return self._cache.get(self.config.token_cache_key)

    @database_sync_to_async
    def _set_cached_token(self, token: str):
        """Set token in cache"""
        self._cache.set(
            self.config.token_cache_key,
            token,
            timeout=self.config.cache_ttl
        )

    async def get_token(self) -> str:
        """Get cached token or fetch new one if expired"""
        token = await self._get_cached_token()
        if token:
            return token

        url = urljoin(self.config.base_url, '/api/token')
        data = {
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret
        }
        headers = {
            'Content-Type': 'application/json'
        }
        
        try:
            response = await self._client.post(
                url, 
                json=data,  # Use json parameter to send JSON data
                headers=headers
            )
            if response.status_code != 200:
                print(f"Token request failed with status {response.status_code}")
                print(f"Response body: {response.text}")
            response.raise_for_status()
            token = response.json()['access_token']
            await self._set_cached_token(token)
            return token
        except (httpx.HTTPError, KeyError) as e:
            if isinstance(e, httpx.HTTPError):
                print(f"Response status: {e.response.status_code}")
                print(f"Response headers: {e.response.headers}")
                print(f"Response body: {e.response.text}")
            raise ValueError(f"Failed to retrieve LEAF API token: {str(e)}")

    async def get_content_info(self, content_id: str, page_no: int, image_type: str = "thumb") -> Optional[Dict]:
        """Fetch content information with automatic token refresh"""
        token = await self.get_token()
        
        # First, construct the image URL that the frontend will use
        image_url = urljoin(
            self.config.base_url,
            f"/api/get_image_by_id_page_no?content_id={content_id}&page_no={page_no}&image_type={image_type}"
        )
        
        # Return both the URL and the token that will be needed to fetch the image
        return {
            "image_url": image_url,
            "auth_token": token,
            "content_id": content_id,
            "page_no": page_no
    }

class ActivityConsumer(AsyncWebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_client: Optional[LeafAPIClient] = None
        self.config = LeafAPIConfig.from_settings()
        self._cache = caches['default']
        self.reconnect_attempt = 0
        self.max_reconnect_delay = 30

    async def connect(self):
        try:
            print(">>> ActivityConsumer.connect called")
            self.user_id = self.scope['url_route']['kwargs']['user_id']
            self.api_client = LeafAPIClient(self.config)
            await self.accept()
            self.activity_stream_task = asyncio.create_task(self.start_activity_stream())
        except Exception as e:
            print(f"Error in connect: {str(e)}")
            await self.close()

    async def disconnect(self, close_code):
        print(f">>> ActivityConsumer.disconnect called with code {close_code}")
        if hasattr(self, 'activity_stream_task'):
            self.activity_stream_task.cancel()
            try:
                await self.activity_stream_task
            except asyncio.CancelledError:
                print(">>> Activity stream task cancelled successfully")
        
        if self.api_client:
            await self.api_client.close()

    def get_activity_label(self, operation_type, contents_name, page_no):
        """Generate a human-readable label based on operation type, contents name, and page number."""
        if operation_type == "page_open":
            return f"{contents_name} (Page {page_no})"
        elif operation_type == "quiz_answer":
            return f"Answered Quiz on {contents_name} (Page {page_no})"
        elif operation_type == "next":
            return f"Navigated to Next Page: {contents_name} (Page {page_no})"
        elif operation_type == "close":
            return f"Closed {contents_name} (Page {page_no})"
        else:
            return f"{operation_type.replace('_', ' ').title()} - {contents_name} (Page {page_no})"

    @database_sync_to_async
    def get_recent_activity(self):
        """Retrieve the initial set of student activities for the dashboard."""
        clickhouse_query = """
            SELECT DISTINCT ON (id)
                id,
                operation_name as type,
                timestamp,
                platform,
                object_id,
                description,
                marker_color,
                marker_position,
                marker_text,
                memo_title,
                memo_text,
                contents_id,
                contents_name,
                page_no,
                context_label 
            FROM statements_mv
            WHERE actor_account_name = %(user_id)s
            AND timestamp >= now() - INTERVAL 1 HOUR
            ORDER BY id, timestamp ASC
            LIMIT 100
        """
        
        with connections['clickhouse_db'].cursor() as ch_cursor:
            ch_cursor.execute(clickhouse_query, {'user_id': self.user_id})
            rows = ch_cursor.fetchall()
        
        return [self._process_activity_row(row) for row in rows]

    @database_sync_to_async
    def get_live_activity(self, last_timestamp):
        """Retrieve new activities that have occurred since the last_timestamp."""
        clickhouse_query = """
            SELECT DISTINCT ON (id)
                id,
                operation_name as type,
                timestamp,
                platform,
                object_id,
                description,
                marker_color,
                marker_position,
                marker_text,
                memo_title,
                memo_text,
                contents_id,
                contents_name,
                page_no,
                context_label  
            FROM statements_mv
            WHERE actor_account_name = %(user_id)s
              AND timestamp > %(last_timestamp)s
            ORDER BY timestamp ASC
        """
        
        with connections['clickhouse_db'].cursor() as ch_cursor:
            ch_cursor.execute(clickhouse_query, {
                'user_id': self.user_id,
                'last_timestamp': last_timestamp
            })
            rows = ch_cursor.fetchall()
        
        return [self._process_activity_row(row) for row in rows]

    def _process_activity_row(self, row):
        """Process a database row into an activity dict."""
        id, type_, timestamp, platform, object_id, description, marker_color, \
        marker_position, marker_text, memo_title, memo_text, contents_id, \
        contents_name, page_no, context_label = row       
    
        
        # Format timestamp as 'YYYY-MM-DD HH:MM:SS.mmm'
        timestamp_formatted = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if timestamp else None
        
        # Generate a human-readable label
        label = self.get_activity_label(type_, contents_name, page_no)
        
        return {
            "id": id,
            "type": type_,
            "timestamp": timestamp_formatted,
            "platform": platform,
            "object_id": object_id,
            "description": description,
            "marker_color": marker_color,
            "marker_position": marker_position,
            "marker_text": marker_text,
            "memo_title": memo_title,
            "memo_text": memo_text,
            "contents_id": contents_id,
            "contents_name": contents_name,
            "page_no": page_no,
            "context_label": context_label,
            "label": label
        }

    def get_cache_key(self, content_id: str, page_no: int) -> str:
        """Generate consistent cache key for content"""
        return f"{self.config.content_cache_prefix}:{content_id}:{page_no}"

    @database_sync_to_async
    def get_cached_content(self, key: str) -> Optional[Dict]:
        """Retrieve cached content information"""
        return self._cache.get(key)

    @database_sync_to_async
    def cache_content(self, key: str, content: Dict):
        """Cache content information"""
        self._cache.set(key, content, timeout=self.config.cache_ttl)

    async def enrich_activity(self, activity: Dict) -> Dict:
        """Enrich activity with cached content information"""
        cache_key = self.get_cache_key(
            activity['contents_id'],
            activity['page_no']
        )
        
        content_info = await self.get_cached_content(cache_key)
        print(f"Content info for {cache_key}: {content_info}")

        if not content_info and self.api_client:
            content_info = await self.api_client.get_content_info(
                activity['contents_id'],
                activity['page_no']
            )
            print(f"Content info from API: {content_info}")
            if content_info:
                await self.cache_content(cache_key, content_info)

        if content_info:
            activity["content_info"] = content_info
        return activity

    async def start_activity_stream(self):
        """Monitor and send activities with improved error handling and backoff"""
        initial_activities = await self.get_recent_activity()
        if initial_activities:
            last_timestamp_iso = initial_activities[-1]['timestamp']
        else:
            threshold_time = timezone.now() - datetime.timedelta(minutes=2)
            last_timestamp_iso = threshold_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        while True:
            try:
                new_activities = await self.get_live_activity(last_timestamp_iso)
                # print(f"Last timestamp: {last_timestamp_iso}")
                # print(f"New activities: {new_activities}")
                self.reconnect_attempt = 0  # Reset reconnect attempts on success
                
                for activity in new_activities:
                    print(f"Sending activity: {activity}")
                    last_timestamp_iso = activity["timestamp"]
                    # First send the basic activity
                    await self.send_json({
                        'type': 'new_activity',
                        'activity': activity,
                    })

                    try:
                        # Then enrich and send if there's additional info
                        enriched_activity = await self.enrich_activity(activity)
                        
                        await self.send_json({
                                'type': 'enriched_activity',
                                'activity': enriched_activity,
                            })
                    except Exception as e:
                        print(f"Error enriching activity: {str(e)}")

                await asyncio.sleep(2)
            except asyncio.CancelledError:
                print(">>> Activity stream task cancelled")
                break
            except Exception as e:
                print(f"Error in activity stream: {str(e)}")
                delay = min(2 ** self.reconnect_attempt, self.max_reconnect_delay)
                self.reconnect_attempt += 1
                await asyncio.sleep(delay)

    async def send_json(self, content: Dict):
        """Helper method to send JSON data"""
        await self.send(text_data=json.dumps(content))