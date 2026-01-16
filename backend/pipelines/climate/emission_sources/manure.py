from typing import Dict, Any
from ..utils.conversions import ch4_to_co2e, n2o_to_co2e, nh3_to_indirect_co2e
from .base_source import EmissionSource

class CattleManureHousingStorage(EmissionSource):
    """
    Calculates emissions from cattle manure in housing and storage.
    This includes:
    - CH4 from anaerobic decomposition
    - N2O from nitrification/denitrification
    - NH3 volatilization (converted to indirect N2O)
    """

    def calculate_co2e(self) -> float:
        """
        Calculate total CO2e emissions from cattle manure in housing and storage.

        Returns:
            float: Total emissions in kg CO2e
        """
        total_co2e = 0.0

        # Calculate CH4 emissions
        ch4_kg = self._calculate_ch4_emissions()
        total_co2e += ch4_to_co2e(ch4_kg)

        # Calculate N2O emissions
        n2o_kg = self._calculate_n2o_emissions()
        total_co2e += n2o_to_co2e(n2o_kg)

        # Calculate indirect N2O from NH3
        nh3_kg = self._calculate_nh3_emissions()
        total_co2e += nh3_to_indirect_co2e(nh3_kg)

        return total_co2e

    def _calculate_ch4_emissions(self) -> float:
        """
        Calculate CH4 emissions from manure storage using IPCC methodology.
        CH4 emissions depend on:
        - VS (Volatile Solids) excretion
        - MCF (Methane Conversion Factor) based on storage system
        - B0 (Maximum CH4 producing capacity)

        Returns:
            float: CH4 emissions in kg
        """
        total_ch4_kg = 0.0

        # Process each cattle type
        for cattle_type in ["jersey_conventional_milk_cow", "heavy_breed_conventional_milk_cow"]:
            num_animals = self.farm_data.get_animal_count(cattle_type)
            if num_animals <= 0:
                continue

            # Get VS excretion rate (kg VS/animal/year)
            vs_excretion = self.farm_data.get_animal_input(cattle_type, 'vs_excretion_kg_year')

            # Get storage system type and duration
            storage_system = self.farm_data.get_manure_storage_system(cattle_type)
            storage_days = self.farm_data.get_manure_storage_days(cattle_type)

            # Get MCF based on storage system
            mcf = self.config.get_factor('manure_factors', f'storage.mcf.{storage_system}')

            # Get B0 (default from IPCC)
            b0 = self.config.get_factor('manure_factors', 'storage.b0')

            # Calculate CH4 emissions for this cattle type
            ch4_kg = (
                num_animals *
                vs_excretion *
                mcf *
                b0 *
                (storage_days / 365.0)  # Adjust for storage duration
            )

            total_ch4_kg += ch4_kg

        return total_ch4_kg

    def _calculate_n2o_emissions(self) -> float:
        """
        Calculate N2O emissions from manure storage.
        N2O emissions depend on:
        - N excretion
        - EF (Emission Factor) based on storage system

        Returns:
            float: N2O emissions in kg
        """
        total_n2o_kg = 0.0

        # Process each cattle type
        for cattle_type in ["jersey_conventional_milk_cow", "heavy_breed_conventional_milk_cow"]:
            num_animals = self.farm_data.get_animal_count(cattle_type)
            if num_animals <= 0:
                continue

            # Get N excretion rate (kg N/animal/year)
            n_excretion = self.farm_data.get_animal_input(cattle_type, 'n_excretion_kg_year')

            # Get storage system type
            storage_system = self.farm_data.get_manure_storage_system(cattle_type)

            # Get EF based on storage system
            ef = self.config.get_factor('manure_factors', f'storage.n2o_emission_factors.{storage_system}')

            # Calculate N2O emissions for this cattle type
            n2o_kg = num_animals * n_excretion * ef
            total_n2o_kg += n2o_kg

        return total_n2o_kg

    def _calculate_nh3_emissions(self) -> float:
        """
        Calculate NH3 emissions from manure storage.
        NH3 emissions depend on:
        - TAN (Total Ammoniacal N) excretion
        - EF (Emission Factor) based on storage system

        Returns:
            float: NH3 emissions in kg
        """
        total_nh3_kg = 0.0

        # Process each cattle type
        for cattle_type in ["jersey_conventional_milk_cow", "heavy_breed_conventional_milk_cow"]:
            num_animals = self.farm_data.get_animal_count(cattle_type)
            if num_animals <= 0:
                continue

            # Get TAN excretion rate (kg TAN/animal/year)
            tan_excretion = self.farm_data.get_animal_input(cattle_type, 'tan_excretion_kg_year')

            # Get storage system type
            storage_system = self.farm_data.get_manure_storage_system(cattle_type)

            # Get EF based on storage system
            ef = self.config.get_factor('manure_factors', f'storage.nh3_emission_factors.{storage_system}')

            # Calculate NH3 emissions for this cattle type
            nh3_kg = num_animals * tan_excretion * ef
            total_nh3_kg += nh3_kg

        return total_nh3_kg

class ManureFieldApplication(EmissionSource):
    def calculate_co2e(self):
        """
        Calculates total CO2e emissions from manure field application.
        Includes:
        - Direct N2O emissions from applied manure
        - Indirect N2O emissions from NH3 volatilization
        - Indirect N2O emissions from N leaching
        """
        total_co2e = 0.0

        # Get manure application data from farm_data
        manure_applications = self.farm_data.get_manure_applications()

        for application in manure_applications:
            # Get application details
            manure_type = application['type']  # e.g., 'deep_liquid', 'solid_storage'
            application_method = application['method']  # e.g., 'injection', 'broadcast'
            season = application['season']  # e.g., 'spring', 'summer'
            n_applied = application['n_applied']  # kg N/ha

            # Calculate direct N2O emissions
            n2o_ef = self.config.get_factor('manure_factors', f'field_application.n2o_emission_factors.{season}')
            direct_n2o_n = n_applied * n2o_ef
            direct_n2o = direct_n2o_n * (44.0 / 28.0)  # Convert N2O-N to N2O
            direct_co2e = n2o_to_co2e(direct_n2o)

            # Calculate indirect N2O from NH3 volatilization
            nh3_ef = self.config.get_factor('manure_factors', f'field_application.nh3_emission_factors.{application_method}')
            nh3_n = n_applied * nh3_ef
            indirect_n2o_from_nh3 = nh3_to_indirect_co2e(nh3_n)

            # Calculate indirect N2O from leaching
            leaching_fraction = self.config.get_factor('manure_factors', f'field_application.n_leaching_fractions.{season}')
            n_leached = n_applied * leaching_fraction
            indirect_n2o_from_leaching = n_leached * 0.0075 * (44.0 / 28.0)  # IPCC EF for N2O from leaching
            indirect_co2e_from_leaching = n2o_to_co2e(indirect_n2o_from_leaching)

            # Sum up all emissions for this application
            total_co2e += direct_co2e + indirect_n2o_from_nh3 + indirect_co2e_from_leaching

        return total_co2e