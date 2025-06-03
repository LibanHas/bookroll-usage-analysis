from datetime import datetime
from typing import Dict, Any
from django.http import HttpRequest


def past_years_context(request: HttpRequest) -> Dict[str, Any]:
    """
    Context processor to make past years data available in all templates.
    This is used for generating dynamic sidebar links.
    """
    current_year = datetime.now().year
    start_year = 2019
    end_year = current_year - 1

    # Generate list of available years (most recent first)
    available_years = list(range(start_year, end_year + 1))
    available_years.reverse()

    return {
        'past_years': available_years,
        'past_years_start': start_year,
        'past_years_end': end_year,
        'current_year': current_year,
    }
