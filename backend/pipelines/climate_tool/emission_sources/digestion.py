from typing import Dict, Any
from ..utils.conversions import ch4_to_co2e
from .base_source import EmissionSource

class CattleEntericFermentation(EmissionSource):
    """
    Calculates CH4 emissions from cattle enteric fermentation.
    This implements the NorFor equation for dairy cows and uses standard values for young stock.
    """

    def calculate_co2e(self) -> float:
        """
        Calculate total CO2e emissions from cattle enteric fermentation.

        Returns:
            float: Total emissions in kg CO2e
        """
        total_ch4_kg = 0.0

        # Calculate emissions from milking cows using NorFor equation
        total_ch4_kg += self._calculate_milking_cows_ch4()

        # Calculate emissions from young stock using standard values
        total_ch4_kg += self._calculate_young_stock_ch4()

        # Convert CH4 to CO2e
        return ch4_to_co2e(total_ch4_kg)

    def _calculate_milking_cows_ch4(self) -> float:
        """
        Calculate CH4 emissions from milking cows using the NorFor equation.

        Returns:
            float: Total CH4 emissions in kg
        """
        total_ch4_kg = 0.0

        # Process each cow type (Jersey and heavy breed)
        for cow_type in ["jersey_conventional_milk_cow", "heavy_breed_conventional_milk_cow"]:
            num_cows = self.farm_data.get_animal_count(cow_type)
            if num_cows <= 0:
                continue

            # Get feed intake and composition data
            feed_intake = self.farm_data.get_animal_input(cow_type, 'feed_intake_kg_dm_day')
            fatty_acid = self.farm_data.get_animal_input(cow_type, 'fatty_acid_g_kg_dm')
            ndf = self.farm_data.get_animal_input(cow_type, 'ndf_g_kg_dm')

            # Get NorFor coefficients from config
            cow_params = self.config.get_factor('enteric_fermentation_cattle', cow_type)
            norfor = cow_params['norfor_coeffs']

            # Apply NorFor equation
            ch4_lactating_daily_kg = (
                norfor['c1'] * feed_intake -
                norfor['c2'] * fatty_acid +
                norfor['c3'] * ndf
            ) / norfor['divisor']

            # Calculate annual emissions per cow
            ch4_annual_kg_per_cow = (
                ch4_lactating_daily_kg * norfor['lactating_days'] +
                norfor['gold_period_ch4_daily_kg'] * norfor['gold_period_days']
            )

            total_ch4_kg += num_cows * ch4_annual_kg_per_cow

        return total_ch4_kg

    def _calculate_young_stock_ch4(self) -> float:
        """
        Calculate CH4 emissions from young stock using standard values.

        Returns:
            float: Total CH4 emissions in kg
        """
        total_ch4_kg = 0.0

        # Get standard CH4 values for young stock
        std_ch4_values = self.config.get_factor(
            'enteric_fermentation_cattle',
            'young_stock_standard_ch4_kg_period'
        )

        # Calculate for each young stock type
        for stock_type, ch4_per_period in std_ch4_values.items():
            num_stock = self.farm_data.get_animal_count(stock_type)
            if num_stock > 0:
                total_ch4_kg += num_stock * ch4_per_period

        return total_ch4_kg