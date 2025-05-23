from abc import ABC, abstractmethod
from typing import Any, Dict

class EmissionSource(ABC):
    def __init__(self, farm_data: Any, config: Any):
        """
        Initializes the emission source.

        Args:
            farm_data: An object containing all farm-specific input data.
            config: An object to load/access configuration data (emission factors, constants).
        """
        self.farm_data = farm_data
        self.config = config

    @abstractmethod
    def calculate_co2e(self) -> float:
        """
        Calculates the total CO2 equivalent emissions for this source.

        Returns:
            float: The total emissions in kg CO2e.
        """
        pass