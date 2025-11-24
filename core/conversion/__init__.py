# core/conversion/__init__.py
from .utils.mapping_loader import load_mapping_and_units

# Load 1 lần duy nhất khi import package
mapping_table, units = load_mapping_and_units()

__all__ = ["mapping_table", "units"]