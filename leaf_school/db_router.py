# db_router.py
# This is very important file for database routing. This file is used to route the database to the respective app. We should avoid accidental writes to the wrong database.

class DatabaseRouter:
    def db_for_read(self, model, **hints):
        """Route read operations based on the app_label or table."""
        if model._meta.app_label == 'moodle_app':  # All models in `moodle_app` go to moodle_db
            return 'moodle_db'
        if model._meta.app_label == 'bookroll_app':
            return 'bookroll_db'
        if model._meta.app_label == 'clickhouse_app':
            return 'clickhouse_db'
        return 'default'

    def db_for_write(self, model, **hints):
        """Disallow write operations for external databases."""
        if model._meta.app_label in ['moodle_app', 'bookroll_app', 'clickhouse_app']:
            return None
        return 'default'

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """Prevent migrations for non-default databases."""
        if db in ['moodle_db', 'bookroll_db', 'clickhouse_db']:
            return False
        return True


