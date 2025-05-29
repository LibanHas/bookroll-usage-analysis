# Course Management App

This Django app manages course information synchronized from the Moodle database.

## Features

- **Course Model**: Stores course information from Moodle including category hierarchy
- **Management Command**: Sync courses from Moodle database with create/update logic
- **Django Admin**: Full admin interface for course management
- **Subject Categories**: Configurable subject categories (English, Mathematics)

## Models

### Course
Stores course information pulled from Moodle database based on the query joining:
- `mdl_course_categories` (parent and child categories)
- `mdl_course` (course details)

**Key Fields:**
- `course_id`: Unique Moodle course ID
- `course_name`: Course full name
- Category information (parent/child IDs and names)
- Course details (sort order, visibility, dates)
- `subject_category`: Custom field for subject classification
- Metadata (created, updated, last_synced timestamps)

## Management Commands

### sync_moodle_courses

Synchronizes course information from Moodle database.

**Usage:**
```bash
# Sync all courses
python manage.py sync_moodle_courses

# Dry run (preview changes without making them)
python manage.py sync_moodle_courses --dry-run

# Sync specific course by ID
python manage.py sync_moodle_courses --course-id 123

# Verbose output
python manage.py sync_moodle_courses --verbose
```

**Features:**
- Create or update logic: Updates existing courses if any field has changed
- Handles timestamp conversion from Unix timestamps to Django datetime
- Skips courses with NULL data from LEFT JOIN
- Comprehensive logging and error handling
- Atomic transactions for data integrity
- Batch processing for large datasets

## Configuration

### Settings

Add to `settings.py`:

```python
# Course subject categories
COURSE_SUBJECT_CATEGORIES = [
    ('english', 'English'),
    ('mathematics', 'Mathematics'),
]

# Course sync settings
COURSE_SYNC_BATCH_SIZE = 100
COURSE_SYNC_TIMEOUT = 300  # 5 minutes
```

### Database Routing

The app uses the `moodle_db` database connection for reading course data. Ensure your database router is configured properly in `DATABASE_ROUTERS`.

## Django Admin

The Course model is fully integrated with Django admin:

- **List View**: Shows key course information with filters and search
- **Detail View**: Organized fieldsets for easy editing
- **Filters**: By subject category, visibility, category names, dates
- **Search**: By course name, ID, and category names
- **Read-only Fields**: Course ID, metadata fields, computed properties

## Database Schema

The Course model creates the `course_course` table with indexes on:
- `course_id` (unique)
- `parent_category_id`
- `child_category_id`
- `subject_category`
- `course_visible`

## Testing

Run tests with:
```bash
python manage.py test course
```

Tests cover:
- Course model creation and validation
- Properties (`is_active`, `full_category_path`)
- String representations

## Future Enhancements

- Frontend course listing and detail views (basic CBV structure is already in place)
- Advanced filtering and search functionality
- Course enrollment tracking
- Integration with other educational platforms
- Automated sync scheduling with celery/cron
- Course analytics and reporting

## Security

- Moodle database is accessed with read-only permissions
- All database operations use atomic transactions
- Input validation and sanitization
- Proper logging for audit trails

## Performance

- Optimized database queries with proper indexing
- Batch processing for large datasets
- Selective field updates to minimize database writes
- Configurable timeout and batch size settings