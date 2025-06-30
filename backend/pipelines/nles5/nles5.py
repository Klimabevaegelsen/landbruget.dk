import logging
from typing import Dict, Optional
import geopandas as gpd
import numpy as np
from ...src.sources.parsers.agricultural_fields import AgriculturalFields
from .percolation import PercolationCalculator
from ...src.sources.static.fertilizer.parser import CatchCrops, FertilizerAccounts, FieldPlanFertilizer
from ...src.sources.parsers.soil_type import SoilTypeParser

logger = logging.getLogger(__name__)

class NLES5Calculator:
    """
    Calculator for estimating nitrogen washout using the NLES5 model.

    It calculates nitrogen washout for agricultural fields based on:
    - Field geometry and crop type
    - Percolation data
    - Catch crop data
    - Soil type and parameters
    """

    def __init__(
        self,
        agricultural_fields_parser: AgriculturalFields,
        percolation_calculator: PercolationCalculator,
        catch_crops_parser: CatchCrops,
        fertilizer_accounts_parser: FertilizerAccounts,
        field_plan_parser: FieldPlanFertilizer,
        soil_type_parser: SoilTypeParser
    ):
        """
        Initialize the NLES5 calculator with required data sources.

        Args:
            agricultural_fields_parser: Parser for agricultural field data
            percolation_calculator: Calculator for percolation data
            catch_crops_parser: Parser for catch crop data
            fertilizer_accounts_parser: Parser for fertilizer accounts
            field_plan_parser: Parser for field plan with fertilizer info
            soil_type_parser: Parser for soil type data
        """
        self.ag_fields = agricultural_fields_parser
        self.percolation = percolation_calculator
        self.catch_crops = catch_crops_parser
        self.fertilizer_accounts = fertilizer_accounts_parser
        self.field_plan = field_plan_parser
        self.soil_type_parser = soil_type_parser

        # Crop parameters
        self.crop_params: Dict[str, float] = {
            'winter_cereals': 0,
            'spring_cereals': -6.74,
            'mixed_cereals_peas': -7.28,
            'grass_clover': -13.49,
            'seed_grass': -17.48,
            'fallow': -11.19,
            'sugar_beets': -0.64,
            'maize_potatoes': 3.53,
            'winter_rape': -7.32,
            'winter_cereals_after_grass': -1.25,
            'maize_after_grass': 19.52,
            'spring_cereals_after_grass': -6.23,
            'pulses_winter_rape': -2.87
        }

        # Coefficients for each model parameter
        self.coefficients: Dict[str, float] = {
            'Bt': 0.456793,
            'Bcs': 0.049570,
            'Bca': 0.157044,
            'Budb': 0.038245,
            'Bm1': 0.026499,
            'Bf0': 0.016314,
            'Bf1': 0.026499,
            'Bg0': 0.014099
        }

        # Soil type parameters
        self.soil_params = {
            'sand': {
                'per1_coef': -0.001194,
                'per2_coef': -0.00111,
                'per_p_coef': -0.00086
            },
            'clay': {
                'per1_coef': -0.00080,
                'per2_coef': -0.00075,
                'per_p_coef': -0.00064
            }
        }

    async def calculate_nitrogen_washout(
        self,
        field_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        Calculate nitrogen washout for a specific field using the NLES5 model.

        Args:
            field_id: The ID of the agricultural field
            start_date: Optional start date for the calculation period
            end_date: Optional end date for the calculation period

        Returns:
            GeoDataFrame containing the nitrogen washout calculation results
        """
        try:
            # 1. Get field data
            field_data = await self.ag_fields.get_field(field_id)
            if field_data.empty:
                logger.error(f"No field data found for field_id: {field_id}")
                return gpd.GeoDataFrame()

            # 2. Get soil type data
            soil_type_data = await self.soil_type_parser.fetch_soil_types()
            if soil_type_data is not None and not soil_type_data.empty:
                # Spatial join to get soil type for the field
                field_with_soil = gpd.sjoin(field_data, soil_type_data, how='left', predicate='intersects')
                if not field_with_soil.empty:
                    field_data = field_with_soil

            # 3. Get percolation data
            percolation_data = await self.percolation.get_daily_percolation(
                bbox=field_data.geometry.bounds,
                start_date=start_date,
                end_date=end_date
            )

            if percolation_data.empty:
                logger.error(f"No percolation data found for field_id: {field_id}")
                return gpd.GeoDataFrame()

            # 4. Get catch crop data
            catch_crop_data = await self.catch_crops.fetch()
            catch_crop_data = catch_crop_data[catch_crop_data['field_id'] == field_id]

            # 5. Get fertilizer account data
            fertilizer_data = await self.fertilizer_accounts.fetch()
            fertilizer_data = fertilizer_data[fertilizer_data['field_id'] == field_id]

            # 6. Get field plan data
            field_plan_data = await self.field_plan.fetch()
            field_plan_data = field_plan_data[field_plan_data['field_id'] == field_id]

            # 7. Calculate nitrogen washout
            result = self._calculate_washout(
                field_data,
                percolation_data,
                catch_crop_data,
                fertilizer_data,
                field_plan_data
            )

            return result

        except Exception as e:
            logger.error(f"Error calculating nitrogen washout: {str(e)}")
            return gpd.GeoDataFrame()

    def _calculate_washout(
        self,
        field_data: gpd.GeoDataFrame,
        percolation_data: gpd.GeoDataFrame,
        catch_crop_data: gpd.GeoDataFrame,
        fertilizer_data: gpd.GeoDataFrame,
        field_plan_data: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Calculate the nitrogen washout using the NLES5 formula.

        Args:
            field_data: Field geometry and crop information
            percolation_data: Percolation data for the field
            catch_crop_data: Catch crop data for the field
            fertilizer_data: Fertilizer account data for the field
            field_plan_data: Field plan data with fertilizer info

        Returns:
            GeoDataFrame with calculation results
        """
        try:
            # 1. Prepare percolation data
            # Split percolation into periods (assuming daily data)
            per1 = percolation_data[percolation_data['date'].dt.month.isin([9, 10, 11])]['percolation'].sum()
            per2 = percolation_data[percolation_data['date'].dt.month.isin([12, 1, 2])]['percolation'].sum()
            per3 = percolation_data[percolation_data['date'].dt.month.isin([3, 4, 5, 6, 7, 8])]['percolation'].sum()

            # Ensure non-negative values
            per1 = max(0, per1)
            per2 = max(0, per2)
            per3 = max(0, per3)

            # Calculate total percolation
            per = per1 + per2 + per3

            # 2. Get crop parameters
            crop_type = field_data['crop_type'].iloc[0]
            m_crop_param = self.crop_params.get(crop_type, 0)

            # 3. Get soil type and parameters
            soil_type = 'sand' if field_data['soil_type'].iloc[0] in [1, 2, 3, 4] else 'clay'
            soil_params = self.soil_params[soil_type]

            # 4. Calculate drainage effect
            drain = (1 - np.exp(soil_params['per1_coef'] * per1 +
                               soil_params['per2_coef'] * (per2 + per3))) * \
                   np.exp(soil_params['per_p_coef'] * (per2 + per3))

            # 5. Calculate soil effect
            soil = np.exp(-0.00185 * field_data['clay_content'].iloc[0])

            # 6. Calculate percolation and soil effect
            perco_soil_effect = drain * soil * 1.085

            # 7. Calculate nitrogen effect
            # Get nitrogen parameters from field plan and fertilizer data
            tn_t_ha = field_plan_data['N Kvote Mark'].iloc[0] if not field_plan_data.empty else 0
            mineral_n_foraar = fertilizer_data['F_706_1'].iloc[0] if not fertilizer_data.empty else 0
            mineral_n_eft = fertilizer_data['F_610'].iloc[0] if not fertilizer_data.empty else 0
            mineral_n_udb = fertilizer_data['F_193'].iloc[0] if not fertilizer_data.empty else 0
            niveau = field_plan_data['Harmoni Areal'].iloc[0] if not field_plan_data.empty else 0
            nfix_ha = 0  # TODO: Get from appropriate source
            niveau_nfix = 0  # TODO: Get from appropriate source
            organic_n_hus = fertilizer_data['F_901'].iloc[0] if not fertilizer_data.empty else 0

            # Calculate nitrogen effect
            n = (self.coefficients['Bt'] * tn_t_ha +
                 self.coefficients['Bcs'] * mineral_n_foraar +
                 self.coefficients['Bca'] * mineral_n_eft +
                 self.coefficients['Budb'] * mineral_n_udb +
                 self.coefficients['Bm1'] * (niveau + niveau) / 2 +
                 self.coefficients['Bf0'] * nfix_ha +
                 self.coefficients['Bf1'] * (niveau_nfix + niveau_nfix) / 2 +
                 self.coefficients['Bg0'] * organic_n_hus)

            # 8. Calculate crop effect
            crop = m_crop_param

            # 9. Calculate trend effect
            trend = -0.1108 * (2017 - 1991)  # Using reference year 2017

            # 10. Calculate final nitrogen washout
            v = 23.51 + n + crop
            vk = v ** 1.5
            y5 = trend + vk * perco_soil_effect

            # 11. Create result GeoDataFrame
            result = gpd.GeoDataFrame({
                'field_id': field_data['field_id'].iloc[0],
                'geometry': field_data['geometry'].iloc[0],
                'nitrogen_washout': y5,
                'percolation_total': per,
                'percolation_period1': per1,
                'percolation_period2': per2,
                'percolation_period3': per3,
                'crop_type': crop_type,
                'soil_type': soil_type,
                'drainage_effect': drain,
                'soil_effect': soil,
                'nitrogen_effect': n,
                'crop_effect': crop,
                'trend_effect': trend,
                'total_nitrogen': tn_t_ha,
                'mineral_n_spring': mineral_n_foraar,
                'mineral_n_autumn': mineral_n_eft,
                'mineral_n_applied': mineral_n_udb,
                'organic_n': organic_n_hus
            }, index=[0])

            return result

        except Exception as e:
            logger.error(f"Error in NLES5 calculation: {str(e)}")
            return gpd.GeoDataFrame()
