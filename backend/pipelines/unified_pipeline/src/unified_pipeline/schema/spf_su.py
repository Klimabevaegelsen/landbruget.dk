"""
Pydantic schemas for SPF SU API response.
"""
from typing import List, Optional, Any
from pydantic import BaseModel


class Address(BaseModel):
    farmName: Optional[str]
    line1: Optional[str]
    postalCode: Optional[str]
    city: Optional[str]
    name: Optional[str]


class DanishCertificate(BaseModel):
    approved: bool
    pdfFileName: str
    isExpired: bool


class HealthData(BaseModel):
    conditionalStatus: str
    healthStatus: str
    healthStatusColor: str
    supplementaryStatus: str


class SalmonellaData(BaseModel):
    salmonellaLevel: List[Any]
    salmonellaIndexes: List[Any]
    hasIndexDetails: bool
    salmonellaDate: str
    salmonellaStatus: str
    salmonellaTestResults: List[Any]
    showData: bool


class OwnerDetailInfo(BaseModel):
    chrNumber: str
    ownerNumber: int
    herdNumber: str
    name: str
    address: Address
    danishCertificate: DanishCertificate
    healthData: HealthData
    salmonellaData: SalmonellaData


class HealthControlInfo(BaseModel):
    disease: str
    lastSample: Optional[str] = None
    nextSample: Optional[str] = None


class HealthStatusModel(BaseModel):
    healthControlInfo: List[HealthControlInfo]
    activeConditionalStatus: List[Any]
    deliveryOptions: List[str]
    receptionOptions: List[str]
    susCoRunningFarms: List[Any]
    veterinarians: Optional[Any] = None


class SpfSuResponse(BaseModel):
    ownerDetailInfo: OwnerDetailInfo
    healthStatus: HealthStatusModel
    hasAccessToDetails: bool
    googleAnalytics: str
