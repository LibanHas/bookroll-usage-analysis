from django import template
from django.urls import reverse, NoReverseMatch
from django.utils.safestring import mark_safe

register = template.Library()


@register.simple_tag
def past_year_url(year):
    """
    Generate a URL for a specific past year.
    Returns a safe URL or '#' if the URL pattern doesn't exist.
    """
    try:
        url_name = f'past_years:year_{year}'
        return reverse(url_name)
    except NoReverseMatch:
        # Return a placeholder if the URL doesn't exist
        return '#'


@register.simple_tag
def past_year_url_safe(year, url_suffix=''):
    """
    Generate a URL for a specific past year with optional suffix.
    Returns a safe URL or '#' if the URL pattern doesn't exist.
    """
    try:
        if url_suffix:
            url_name = f'past_years:year_{year}_{url_suffix}'
        else:
            url_name = f'past_years:year_{year}'
        return reverse(url_name)
    except NoReverseMatch:
        # Return a placeholder if the URL doesn't exist
        return '#'


@register.simple_tag
def past_year_clear_cache_url(year):
    """
    Generate a cache clear URL for a specific past year.
    Returns a safe URL or '#' if the URL pattern doesn't exist.
    """
    try:
        url_name = f'past_years:year_{year}_clear_cache'
        return reverse(url_name)
    except NoReverseMatch:
        # Return a placeholder if the URL doesn't exist
        return '#'