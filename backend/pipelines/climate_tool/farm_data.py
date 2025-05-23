from typing import Dict, Any, Optional
from dataclasses import dataclass, field

@dataclass
class FarmData:
    """
    Stores and provides access to farm-specific data used in emission calculations.
    """
    # Animal data
    animal_counts: Dict[str, int] = field(default_factory=dict)
    animal_inputs: Dict[tuple[str, str], float] = field(default_factory=dict)

    # Field/crop data
    field_areas: Dict[str, float] = field(default_factory=dict)  # ha
    crop_yields: Dict[str, float] = field(default_factory=dict)  # kg/ha
    fertilizer_applications: Dict[str, float] = field(default_factory=dict)  # kg N/ha

    # Energy use data
    energy_use: Dict[str, float] = field(default_factory=dict)  # MJ or liters

    def get_animal_count(self, animal_type: str) -> int:
        """
        Get the number of animals of a specific type.

        Args:
            animal_type: Identifier for the animal type (e.g., 'jersey_conventional_milk_cow')

        Returns:
            int: Number of animals
        """
        return self.animal_counts.get(animal_type, 0)

    def get_animal_input(self, animal_type: str, input_type: str) -> float:
        """
        Get a specific input value for an animal type.

        Args:
            animal_type: Identifier for the animal type
            input_type: Type of input (e.g., 'feed_intake_kg_dm_day')

        Returns:
            float: Input value
        """
        return self.animal_inputs.get((animal_type, input_type), 0.0)

    def get_area(self, field_type: str) -> float:
        """
        Get the area of a specific field type.

        Args:
            field_type: Identifier for the field type

        Returns:
            float: Area in hectares
        """
        return self.field_areas.get(field_type, 0.0)

    def get_crop_yield(self, crop_type: str) -> float:
        """
        Get the yield for a specific crop type.

        Args:
            crop_type: Identifier for the crop type

        Returns:
            float: Yield in kg/ha
        """
        return self.crop_yields.get(crop_type, 0.0)

    def get_fertilizer_application(self, field_type: str) -> float:
        """
        Get the fertilizer application rate for a specific field type.

        Args:
            field_type: Identifier for the field type

        Returns:
            float: Application rate in kg N/ha
        """
        return self.fertilizer_applications.get(field_type, 0.0)

    def get_energy_use(self, energy_type: str) -> float:
        """
        Get the energy use for a specific type.

        Args:
            energy_type: Identifier for the energy type (e.g., 'diesel_liters')

        Returns:
            float: Energy use in MJ or liters
        """
        return self.energy_use.get(energy_type, 0.0)

    def has_category(self, category: str) -> bool:
        """
        Check if the farm has any data in a specific category.

        Args:
            category: Category to check ('cattle', 'pigs', 'crops', etc.)

        Returns:
            bool: True if the farm has data in this category
        """
        if category == 'cattle':
            return any('cow' in k or 'opdraet' in k for k in self.animal_counts.keys())
        elif category == 'pigs':
            return any('svin' in k for k in self.animal_counts.keys())
        elif category == 'crops':
            return len(self.field_areas) > 0
        elif category == 'energy':
            return len(self.energy_use) > 0
        return False