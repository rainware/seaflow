from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _

__all__ = ['SeaflowConfig']

class SeaflowConfig(AppConfig):
    name = 'seaflow'
    verbose_name = _("Seaflow")
