# Django Channels consumer for real-time activity stream
# consumer.py

import json
import asyncio
import datetime
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from django.utils import timezone
from django.db import connections

class ActivityConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        print(">>> ActivityConsumer.connect called")
        self.user_id = self.scope['url_route']['kwargs']['user_id']
        # Optional: Add authentication checks here
        await self.accept()
        # Send initial activities
        # await self.send_initial_activities()
        # Start the activity stream as a background task
        self.activity_stream_task = asyncio.create_task(self.start_activity_stream())

    async def disconnect(self, close_code):
        print(f">>> ActivityConsumer.disconnect called with code {close_code}")
        # Cancel the activity stream task if it's running
        if hasattr(self, 'activity_stream_task'):
            self.activity_stream_task.cancel()
            try:
                await self.activity_stream_task
            except asyncio.CancelledError:
                print(">>> Activity stream task cancelled successfully")

    @database_sync_to_async
    def get_recent_activity(self):
        """
        Retrieve the initial set of student activities for the dashboard.
        """
        clickhouse_query = """
            SELECT DISTINCT ON (id)
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
        
        activities = []
        for row in rows:
            type_ = row[0]
            timestamp = row[1]
            platform = row[2]
            object_id = row[3]
            description = row[4]
            marker_color = row[5]
            marker_position = row[6]
            marker_text = row[7]
            memo_title = row[8]
            memo_text = row[9]
            contents_id = row[10]
            contents_name = row[11]
            page_no = row[12]
            context_label = row[13]
            
            # Ensure timestamp is timezone-aware
            if timestamp is not None and timestamp.tzinfo is None:
                timestamp = timezone.make_aware(
                    timestamp,
                    timezone.get_default_timezone()
                )
            
            # Format timestamp as 'YYYY-MM-DD HH:MM:SS.mmm'
            timestamp_formatted = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if timestamp else None
            
            # Generate a human-readable label
            label = self.get_activity_label(type_, contents_name, page_no)
            
            activity = {
                "type": type_,
                "timestamp": timestamp_formatted,  # Use formatted timestamp
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
            
            activities.append(activity)
        
        return activities

    @database_sync_to_async
    def get_live_activity(self, last_timestamp):
        """
        Retrieve new activities that have occurred since the last_timestamp.
        """
        clickhouse_query = """
            SELECT DISTINCT ON (id)
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
        
        activities = []
        for row in rows:
            type_ = row[0]
            timestamp = row[1]
            platform = row[2]
            object_id = row[3]
            description = row[4]
            marker_color = row[5]
            marker_position = row[6]
            marker_text = row[7]
            memo_title = row[8]
            memo_text = row[9]
            contents_id = row[10]
            contents_name = row[11]
            page_no = row[12]
            context_label = row[13]
            
            # Ensure timestamp is timezone-aware
            if timestamp is not None and timestamp.tzinfo is None:
                timestamp = timezone.make_aware(
                    timestamp,
                    timezone.get_default_timezone()
                )
            
            # Format timestamp as 'YYYY-MM-DD HH:MM:SS.mmm'
            timestamp_formatted = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if timestamp else None
            
            # Generate a human-readable label
            label = self.get_activity_label(type_, contents_name, page_no)
            
            activity = {
                "type": type_,
                "timestamp": timestamp_formatted,  # Use formatted timestamp
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
            
            activities.append(activity)
        
        return activities

    def get_activity_label(self, operation_type, contents_name, page_no):
        """
        Generate a human-readable label based on operation type, contents name, and page number.
        """
        if operation_type == "page_open":
            return f"{contents_name} (Page {page_no})"
        elif operation_type == "quiz_answer":
            return f"Answered Quiz on {contents_name} (Page {page_no})"
        elif operation_type == "next":
            return f"Navigated to Next Page: {contents_name} (Page {page_no})"
        elif operation_type == "close":
            return f"Closed {contents_name} (Page {page_no})"
        # Add more operation types as needed
        else:
            return f"{operation_type.replace('_', ' ').title()} - {contents_name} (Page {page_no})"


    async def start_activity_stream(self):
        """
        Continuously monitor and send new activities to the client.
        """
        # Initialize last_timestamp to the latest timestamp from initial activities
        initial_activities = await self.get_recent_activity()
        if initial_activities:
            last_timestamp_iso = initial_activities[-1]['timestamp']
        else:
            # If no initial activities, set to 2 minutes ago
            threshold_time = timezone.now() - datetime.timedelta(minutes=2)
            last_timestamp_iso = threshold_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        while True:
            try:
                new_activities = await self.get_live_activity(last_timestamp_iso)
                if new_activities:
                    for activity in new_activities:
                        # Update the last_timestamp_iso to the latest activity's timestamp
                        activity_timestamp_iso = activity["timestamp"]
                        if activity_timestamp_iso:
                            # Parse the timestamp back to datetime object
                            last_timestamp = timezone.datetime.strptime(activity_timestamp_iso, '%Y-%m-%d %H:%M:%S.%f')
                            # Make it timezone-aware in UTC
                            last_timestamp = timezone.make_aware(last_timestamp, timezone.utc)
                            last_timestamp_iso = last_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        
                        await self.send(text_data=json.dumps({
                            'type': 'new_activity',
                            'activity': activity
                        }))
                await asyncio.sleep(2)  # Poll every 2 seconds
            except asyncio.CancelledError:
                print(">>> Activity stream task cancelled")
                break
            except Exception as e:
                print(f">>> Error in activity stream: {e}")
                await asyncio.sleep(2)  # Wait before retrying
