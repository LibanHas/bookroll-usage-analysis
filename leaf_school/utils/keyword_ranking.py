import re
import logging
import pandas as pd
import collections
from contextlib import contextmanager
import spacy
from tqdm import tqdm
from leaf_school.utils.db_helpers import clickhouse_connection

logger = logging.getLogger(__name__)

# Define constants
LANGUAGE_MODELS = {}
STOP_WORDS = {
    'en': set(),
    'ja': set(["これ", "それ", "こと", "もの"])
}

# Flag to track if we've already logged the KeyBERT warning
KEYBERT_WARNING_LOGGED = False

# Set this to False to see all debug messages
REDUCE_LOG_VERBOSITY = True

# Global variable to store KeyBERT model instance (singleton)
KEYBERT_MODEL = None
# Lock to prevent concurrent model loading
KEYBERT_MODEL_LOADING = False

def log_once(message, level=logging.WARNING):
    """Log a message only once in the application lifecycle"""
    global KEYBERT_WARNING_LOGGED
    if not KEYBERT_WARNING_LOGGED:
        if level == logging.ERROR:
            logger.error(message)
        elif level == logging.WARNING:
            logger.warning(message)
        elif level == logging.INFO:
            logger.info(message)
        KEYBERT_WARNING_LOGGED = True


def load_language_models():
    """
    Lazily load language models to save memory.
    Models are loaded only when needed.
    """
    global LANGUAGE_MODELS, STOP_WORDS

    if 'en' not in LANGUAGE_MODELS:
        try:
            # Load English model with minimal pipeline for efficiency
            LANGUAGE_MODELS['en'] = spacy.load("en_core_web_sm", disable=["ner", "parser"])
            STOP_WORDS['en'] = LANGUAGE_MODELS['en'].Defaults.stop_words
            if not REDUCE_LOG_VERBOSITY:
                logger.info("Loaded English language model")
        except Exception as e:
            logger.error(f"Failed to load English model: {e}")
            logger.error("Please run: python -m spacy download en_core_web_sm")

    if 'ja' not in LANGUAGE_MODELS:
        try:
            # First try the standard Japanese model (ja_core_news_sm)
            LANGUAGE_MODELS['ja'] = spacy.load("ja_core_news_sm")
            if not REDUCE_LOG_VERBOSITY:
                logger.info("Loaded Japanese language model (ja_core_news_sm)")
        except Exception as e:
            try:
                # Try alternative model name
                LANGUAGE_MODELS['ja'] = spacy.load("ja")
                if not REDUCE_LOG_VERBOSITY:
                    logger.info("Loaded Japanese language model (ja)")
            except Exception as e2:
                try:
                    # Try ginza as a last resort
                    import ginza
                    LANGUAGE_MODELS['ja'] = spacy.load("ja_ginza")
                    if not REDUCE_LOG_VERBOSITY:
                        logger.info("Loaded Japanese language model (ja_ginza)")
                except Exception as e3:
                    if not REDUCE_LOG_VERBOSITY:
                        logger.warning(f"Failed to load any Japanese model. Errors: {e}, {e2}, {e3}")
                        logger.warning("To enable Japanese support, run: pip install ja_core_news_sm")


def clean_text(text):
    """
    Clean highlight text by removing extra whitespace and control characters.

    Args:
        text (str): Raw highlight text

    Returns:
        str: Cleaned text or None if empty
    """
    if not text:
        return None

    # Replace multiple whitespace and Japanese space characters with a single space
    cleaned = re.sub(r"[\s\u3000]+", " ", text).strip()
    return cleaned if cleaned else None


def detect_language(text):
    """
    Detect the language of the text (simple heuristic).

    Args:
        text (str): Text to analyze

    Returns:
        str: 'en' for English, 'ja' for Japanese (default)
    """
    # If text is mostly ASCII, assume English
    if re.fullmatch(r"[ -~]+", text):
        return 'en'

    # Check if Japanese model is available, otherwise fallback to English
    if 'ja' not in LANGUAGE_MODELS:
        if not REDUCE_LOG_VERBOSITY:
            logger.debug("Japanese model not available, treating as English")
        return 'en'

    return 'ja'


def extract_candidates(text):
    """
    Extract keyword candidates based on language-specific rules.

    Args:
        text (str): Text to analyze

    Returns:
        list: List of candidate keywords/phrases
    """
    if not text:
        return []

    # Ensure models are loaded
    load_language_models()

    lang = detect_language(text)

    # If model not available, return empty list
    if lang not in LANGUAGE_MODELS:
        if not REDUCE_LOG_VERBOSITY:
            logger.warning(f"Language model for {lang} not available")
        return []

    nlp = LANGUAGE_MODELS[lang]
    stop_words = STOP_WORDS[lang]

    if lang == 'en':
        try:
            doc = nlp(text.lower())
            # For English, return noun chunks or words if no noun chunks
            chunks = []

            # Try to use noun chunks if available
            try:
                chunks = [c.text for c in doc.noun_chunks
                        if c.text not in stop_words and len(c.text) > 1]
            except Exception as e:
                if not REDUCE_LOG_VERBOSITY:
                    logger.warning(f"Noun chunks not available: {e}")

            # If no noun chunks found, use individual tokens
            if not chunks:
                chunks = [token.text for token in doc
                        if token.is_alpha and not token.is_stop and len(token.text) > 1]

            # Also include named entities
            try:
                entities = [ent.text.lower() for ent in doc.ents if len(ent.text) > 1]
                chunks.extend(entities)
            except Exception:
                pass

            return chunks
        except Exception as e:
            if not REDUCE_LOG_VERBOSITY:
                logger.error(f"Error extracting English candidates: {e}")
            return []
    else:  # Japanese
        try:
            doc = nlp(text)

            # Extract Japanese candidates
            # For ja_core_news_sm which doesn't have bunsetsu_phrases
            # We use nouns and proper nouns
            candidates = []

            # Try using noun compounds
            for token in doc:
                # Include nouns, proper nouns, and compound nouns
                if token.pos_ in ('NOUN', 'PROPN') and len(token.text) > 1:
                    # Check if it's not a stop word
                    if token.text not in stop_words:
                        candidates.append(token.text)

            # Also include named entities
            try:
                entities = [ent.text for ent in doc.ents if len(ent.text) > 1]
                candidates.extend(entities)
            except Exception:
                pass

            # Try to get noun phrases by looking for adjective + noun patterns
            noun_phrases = []
            for i in range(len(doc) - 1):
                if doc[i].pos_ == 'ADJ' and doc[i+1].pos_ in ('NOUN', 'PROPN'):
                    phrase = doc[i].text + doc[i+1].text
                    if len(phrase) > 1 and phrase not in stop_words:
                        noun_phrases.append(phrase)

            candidates.extend(noun_phrases)

            return candidates
        except Exception as e:
            if not REDUCE_LOG_VERBOSITY:
                logger.error(f"Error extracting Japanese candidates: {e}")
            return []


def initialize_keybert_model():
    """
    Initialize the KeyBERT model as a singleton.
    This function should be called only once.
    """
    global KEYBERT_MODEL, KEYBERT_WARNING_LOGGED, KEYBERT_MODEL_LOADING

    # If model is already being loaded by another thread, wait for it to finish
    if KEYBERT_MODEL_LOADING:
        return None

    # Mark that we're currently loading the model to prevent concurrent loads
    KEYBERT_MODEL_LOADING = True

    try:
        # First, verify all dependencies are available
        import torch
        import transformers
        import sentence_transformers
        from keybert import KeyBERT

        # Try multiple models in order of preference
        models_to_try = [
            "intfloat/multilingual-e5-small",   # Best multilingual model (small)
            "distilbert-base-nli-mean-tokens",  # English fallback (smaller)
            "all-MiniLM-L6-v2"                 # Another reliable option
        ]

        last_error = None
        for model_name in models_to_try:
            try:
                logger.info(f"Attempting to load KeyBERT with model: {model_name}")
                model = KeyBERT(model_name)
                logger.info(f"Successfully loaded KeyBERT with model: {model_name}")
                return model
            except Exception as e:
                last_error = e
                logger.warning(f"Failed to initialize KeyBERT with model {model_name}: {e}")
                continue

        # If we get here, all models failed
        if last_error:
            logger.error(f"All KeyBERT models failed to load. Last error: {last_error}")

        log_once("All KeyBERT models failed. Using frequency-based keyword extraction instead.", logging.WARNING)
        return None

    except ImportError as e:
        log_once(f"KeyBERT dependency missing: {e}. Using frequency-based keyword extraction instead.", logging.WARNING)
        return None
    except Exception as e:
        if not KEYBERT_WARNING_LOGGED:
            logger.error(f"Failed to initialize KeyBERT model: {e}")
            log_once("Using fallback keyword extraction method (frequency-based)", logging.WARNING)
        return None
    finally:
        # Mark that we're done loading the model
        KEYBERT_MODEL_LOADING = False


def get_keybert_model():
    """
    Get or initialize the KeyBERT model.
    Uses singleton pattern to avoid reloading the model multiple times.

    Returns:
        KeyBERT: Initialized KeyBERT model with multilingual support or None if failed
    """
    global KEYBERT_MODEL

    # Return existing model if already loaded
    if KEYBERT_MODEL is not None:
        return KEYBERT_MODEL

    # Initialize model if not yet loaded
    KEYBERT_MODEL = initialize_keybert_model()
    return KEYBERT_MODEL


def tfidf_extract_keywords(text, candidates, max_keywords=5):
    """
    Extract keywords using a simple TF-IDF inspired approach.
    This is a fallback when KeyBERT is not available.

    Args:
        text (str): The text to analyze
        candidates (list): List of candidate phrases
        max_keywords (int): Maximum number of keywords to extract

    Returns:
        list: List of (keyword, score) tuples
    """
    # Count word frequencies
    counter = collections.Counter(candidates)
    total = sum(counter.values())

    # Get document frequency (how many unique words)
    doc_len = len(set(candidates))

    # Calculate a simple score based on frequency and uniqueness
    scores = {}
    for word, count in counter.items():
        # Higher score for more frequent words
        term_freq = count / total if total > 0 else 0
        # Higher score for more unique words in the context
        inverse_doc_freq = 1.0
        if doc_len > 1:
            inverse_doc_freq = len(word.split()) / doc_len

        scores[word] = term_freq * (1 + inverse_doc_freq)

    # Sort by score and return top N
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:max_keywords]


def extract_keywords(text, candidates, max_keywords=5):
    """
    Extract top keywords from text using KeyBERT or fallback method.

    Args:
        text (str): Text to analyze
        candidates (list): Candidate phrases to consider
        max_keywords (int): Maximum number of keywords to extract

    Returns:
        list: List of (keyword, score) tuples
    """
    if not text or not candidates:
        return []

    model = get_keybert_model()
    if not model:
        # Fallback to simple TF-IDF inspired method
        return tfidf_extract_keywords(text, candidates, max_keywords)

    try:
        return model.extract_keywords(
            text,
            candidates=candidates,
            keyphrase_ngram_range=(1, 4),
            use_mmr=True,
            diversity=0.5,
            top_n=max_keywords
        )
    except Exception as e:
        if not REDUCE_LOG_VERBOSITY:
            logger.error(f"KeyBERT extraction error: {e}")
        # Fallback to simple TF-IDF inspired method
        return tfidf_extract_keywords(text, candidates, max_keywords)


def get_student_highlights(context_id=None, limit=None):
    """
    Retrieve student highlights from ClickHouse database.

    Args:
        context_id (str, optional): Filter by specific context (course) ID
        limit (int, optional): Limit number of results

    Returns:
        pandas.DataFrame: DataFrame containing highlight data
    """
    try:
        query = """
        SELECT
            actor_name_id,
            marker_text,
            contents_id,
            contents_name,
            object_id,
            actor_account_name,
            context_id
        FROM statements_mv
        WHERE operation_name = 'ADD_MARKER'
        """

        # Add filters if provided
        if context_id:
            query += f" AND context_id = '{context_id}'"

        # Add limit if provided
        if limit:
            query += f" LIMIT {limit}"

        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                columns = [col[0] for col in cursor.description]
                results = cursor.fetchall()

        # Convert to DataFrame
        df = pd.DataFrame(results, columns=columns)

        # Clean the text
        df["clean_text"] = df["marker_text"].apply(clean_text)

        # Remove rows with empty text
        df = df.dropna(subset=["clean_text"])

        return df

    except Exception as e:
        logger.error(f"Error retrieving highlights: {e}")
        return pd.DataFrame()


def extract_and_rank_keywords(df, max_keywords_per_text=5, top_n=100):
    """
    Process dataframe with highlights to extract and rank keywords.

    Args:
        df (pandas.DataFrame): DataFrame with highlight data
        max_keywords_per_text (int): Maximum keywords to extract per highlight
        top_n (int): Number of top keywords to return in final ranking

    Returns:
        pandas.DataFrame: DataFrame with ranked keywords
    """
    if df.empty:
        return pd.DataFrame(columns=["keyword", "frequency", "score"])

    # Extract candidates
    df["candidates"] = df["clean_text"].apply(extract_candidates)

    # Count empty candidates
    empty_candidates = df["candidates"].apply(lambda x: len(x) == 0).sum()
    if empty_candidates > 0 and not REDUCE_LOG_VERBOSITY:
        logger.warning(f"{empty_candidates} out of {len(df)} highlights had no keyword candidates")

    # Remove rows with empty candidates
    df = df[df["candidates"].apply(lambda x: len(x) > 0)]

    if df.empty:
        logger.warning("No valid candidates found in any highlights")
        return pd.DataFrame(columns=["keyword", "frequency", "score"])

    # Extract keywords
    keyword_results = []
    # Only show progress bar if not reducing verbosity
    iterable = tqdm(df.iterrows(), total=len(df), desc="Extracting keywords") if not REDUCE_LOG_VERBOSITY else df.iterrows()
    for idx, row in iterable:
        keywords = extract_keywords(
            row["clean_text"],
            row["candidates"],
            max_keywords=max_keywords_per_text
        )
        keyword_results.append(keywords)

    df["keywords"] = keyword_results

    # Aggregate rankings
    counter = collections.Counter()
    score_tracker = {}

    for kws in df["keywords"]:
        for kw, score in kws:
            counter[kw] += 1
            # Track the highest score for each keyword
            if kw not in score_tracker or score > score_tracker[kw]:
                score_tracker[kw] = score

    # Get the most common keywords
    ranking = counter.most_common(top_n)

    # Create result DataFrame with frequencies and scores
    result_df = pd.DataFrame(ranking, columns=["keyword", "frequency"])
    result_df["score"] = result_df["keyword"].map(score_tracker)

    return result_df


def get_keyword_ranking(context_id=None, limit=None, max_keywords_per_text=5, top_n=100):
    """
    Main function to retrieve and rank keywords from student highlights.

    Args:
        context_id (str, optional): Filter by specific context (course) ID
        limit (int, optional): Limit number of highlight records to process
        max_keywords_per_text (int): Maximum keywords to extract per highlight
        top_n (int): Number of top keywords to return in final ranking

    Returns:
        pandas.DataFrame: DataFrame with ranked keywords
    """
    # Get highlights from database
    highlights_df = get_student_highlights(context_id, limit)

    if highlights_df.empty:
        if not REDUCE_LOG_VERBOSITY:
            logger.warning(f"No highlights found for context_id={context_id}")
        return pd.DataFrame(columns=["keyword", "frequency", "score"])

    # Extract and rank keywords
    keyword_ranking = extract_and_rank_keywords(
        highlights_df,
        max_keywords_per_text=max_keywords_per_text,
        top_n=top_n
    )

    return keyword_ranking