from .agricultural_fields import AgriculturalFields
from .antibiotics import VetStatAntibioticsParser
from .bnbo_status import BNBOStatus
from .cadastral import Cadastral
from .chr_data import CHRDataParser
from .water_projects import WaterProjects
from .wetlands import Wetlands


def get_source_handler(source_id: str, config: dict):
    """Get appropriate source handler based on source ID"""
    handlers = {
        "water_projects": WaterProjects,
        "wetlands": Wetlands,
        "cadastral": Cadastral,
        "agricultural_fields": AgriculturalFields,
        "chr_data": CHRDataParser,
        "bnbo_status": BNBOStatus,
        "antibiotics": VetStatAntibioticsParser,
    }

    handler_class = handlers.get(source_id)
    if handler_class:
        return handler_class(config)
    return None
