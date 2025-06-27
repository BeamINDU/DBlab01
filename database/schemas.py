from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from fastapi import UploadFile, Form, File, Body

class RoleCreate(BaseModel):
    rolename: str = Field(alias="roleName")
    roledescription: Optional[str] = Field(default=None, alias="description")
    rolestatus: Optional[bool] = Field(default=True, alias="status")
    createdby: Optional[str] = Field(default=None, alias="createdBy")
    # createddate: Optional[datetime] = Field(default_factory=datetime.now, alias="createdDate")

class RoleUpdate(BaseModel):
    rolename: Optional[str] = Field(default=None, alias="roleName")
    roledescription: Optional[str] = Field(default=None, alias="description")
    rolestatus: Optional[bool] = Field(default=None, alias="status")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")
    # updateddate: Optional[datetime] = Field(default_factory=datetime.now, alias="updatedDate")

class ProductCreate(BaseModel):
    prodid: str = Field(alias="productId")
    prodname: Optional[str] = Field(default=None, alias="productName")
    prodtypeid: str = Field(alias="productTypeId")
    prodserial: Optional[str] = Field(default=None, alias="serialNo")
    prodstatus: Optional[bool] = Field(default=True, alias="status")
    createdby: Optional[str] = Field(default=None, alias="createdBy")
    # createddate: Optional[datetime] = Field(default=None, alias="createdDate")

class ProductUpdate(BaseModel):
    prodid: str = Field(alias="productId")
    prodname: Optional[str] = Field(default=None, alias="productName")
    prodtypeid: Optional[str] = Field(default=None, alias="productTypeId")
    prodtype: Optional[str] = Field(default=None, alias="productType")
    prodserial: Optional[str] = Field(default=None, alias="serialNo")
    prodstatus: Optional[bool] = Field(default=None, alias="status")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")
    # updateddate: Optional[datetime] = Field(default_factory=datetime.now, alias="updatedDate")

class ProdTypeCreate(BaseModel):
    prodtypeid: str = Field(alias="productTypeId")
    prodtype: str = Field(alias="productTypeName")
    proddescription: Optional[str] = Field(default=None, alias="description")
    prodstatus: Optional[bool] = Field(default=True, alias="status")
    createdby: Optional[str] = Field(default=None, alias="createdBy")
    # createddate: Optional[datetime] = Field(default=None, alias="createdDate")

class ProdTypeUpdate(BaseModel):
    prodtypeid: str = Field(alias="productTypeId")
    prodtype: Optional[str] = Field(default=None, alias="productTypeName")
    proddescription: Optional[str] = Field(default=None, alias="description")
    prodstatus: Optional[bool] = Field(default=None, alias="status")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")
    # updateddate: Optional[datetime] = Field(default_factory=datetime.now, alias="updatedDate")

class CameraCreate(BaseModel):
    cameraid: str = Field(alias="cameraId")
    cameraname: str = Field(alias="cameraName")
    cameralocation: Optional[str] = Field(default=None, alias="location")
    camerastatus: Optional[bool] = Field(default=True, alias="status")
    createdby: Optional[str] = Field(default=None, alias="createdBy")
    # createddate: Optional[datetime] = Field(default=None, alias="createdDate")

class CameraUpdate(BaseModel):
    cameraid: str = Field(alias="cameraId")
    cameraname: Optional[str] = Field(default=None, alias="cameraName")
    cameralocation: Optional[str] = Field(default=None, alias="location")
    camerastatus: Optional[bool] = Field(default=None, alias="status")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")
    # updateddate: Optional[datetime] = Field(default_factory=datetime.now, alias="updatedDate")

class UserCreate(BaseModel):
    userid: str = Field(alias="userId")
    username: Optional[str] = Field(default=None)
    ufname: Optional[str] = Field(default=None, alias="firstname")
    ulname: Optional[str] = Field(default=None, alias="lastname")
    upassword: Optional[str] = None
    email: Optional[str] = None
    userstatus: Optional[bool] = Field(default=True, alias="status")
    roles: Optional[List[int]] = None
    createdby: Optional[str] = Field(default=None, alias="createdBy")
    # createddate: Optional[datetime] = Field(default=None, alias="createdDate")

class UserUpdate(BaseModel):
    userid: str = Field(alias="userId")
    username: Optional[str] = None
    ufname: Optional[str] = Field(default=None, alias="firstname")
    ulname: Optional[str] = Field(default=None, alias="lastname")
    upassword: Optional[str] = None
    email: Optional[str] = None
    userstatus: Optional[bool] = Field(default=None, alias="status")
    roles: Optional[List[int]] = None
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")
    # updateddate: Optional[datetime] = Field(default_factory=datetime.now, alias="updatedDate")

class DefectTypeCreate(BaseModel):
    defectid: str = Field(alias="defectTypeId")
    defecttype: str = Field(alias="defectTypeName")
    defectdescription: Optional[str] = Field(default=None, alias="description")
    defectstatus: Optional[bool] = Field(default=True, alias="status")
    createdby: Optional[str] = Field(default=None, alias="createdBy")
    # createddate: Optional[datetime] = Field(default=None, alias="createdDate")

class DefectTypeUpdate(BaseModel):
    defectid: str = Field(alias="defectTypeId")
    defecttype: Optional[str] = Field(default=None, alias="defectTypeName")
    defectdescription: Optional[str] = Field(default=None, alias="description")
    defectstatus: Optional[bool] = Field(default=None, alias="status")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")
    # updateddate: Optional[datetime] = Field(default_factory=datetime.now, alias="updatedDate")

class PlanningCreate(BaseModel):
    planid: str = Field(alias="planId")
    prodid: str = Field(alias="productId")
    prodlot: str = Field(alias="lotNo")
    prodline: str = Field(alias="lineId")
    quantity: int
    startdatetime: datetime = Field(alias="startDate")
    enddatetime: datetime = Field(alias="endDate")
    createdby: Optional[str] = Field(default=None, alias="createdBy")

class PlanningUpdate(BaseModel):
    planid: str = Field(alias="planId")
    prodid: str = Field(alias="productId")
    prodlot: str = Field(alias="lotNo")
    prodline: str = Field(alias="lineId")
    quantity: int
    startdatetime: datetime = Field(alias="startDate")
    enddatetime: datetime = Field(alias="endDate")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelCreate(BaseModel):
    modelname: str = Field(alias="modelName")
    modeldescription: Optional[str] = Field(default=None, alias="description")
    prodid: str = Field(alias="ProductId")
    createdby: Optional[str] = Field(default=None, alias="createdBy")

class DetectionModelUpdate(BaseModel):
    modelid: int = Field(alias="modelId")
    modelname: Optional[str] = Field(default=None, alias="modelName")
    modeldescription: Optional[str] = Field(default=None, alias="description")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelUpdateStep1(BaseModel):
    modelid: int = Field(alias="modelId")
    functions: Optional[List[int]] = Field(default=None, alias="functions")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelUpdateStep2(BaseModel):
    modelid: int = Field(alias="modelId")
    modelname: Optional[str] = Field(default=None, alias="modelName")
    modeldescription: Optional[str] = Field(default=None, alias="description")
    trainpercent: Optional[int] = Field(default=None, alias="trainDataset")
    testpercent: Optional[int] = Field(default=None, alias="testDataset")
    valpercent: Optional[int] = Field(default=None, alias="validationDataset")
    epochs: Optional[int] = Field(default=None, alias="epochs")
    prodid: Optional[str] = Field(default=None, alias="ProductId")
    cameraid: Optional[str] = Field(default=None, alias="cameraId")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelUpdateStep3(BaseModel):
    modelid: int = Field(alias="modelId")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelUpdateStep4(BaseModel):
    modelid: int = Field(alias="modelId")
    versionno: Optional[int] = Field(default=None, alias="version")
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")

class DetectionModelImage(BaseModel):
    modelversionid: int = Field(alias="modelVersionId")
    prodid: str = Field(alias="productId")
    cameraid: str = Field(alias="cameraId")
    modelid: int = Field(alias="modelId")
    updatedby: str = Field(alias="updatedBy")
    annotate: Optional[dict] = {}
    filename: str
    base64: str

class TransactionCreate(BaseModel):
    runningno: int = Field(alias="runningNo")
    startdate: datetime = Field(alias="startDate")
    enddate: datetime = Field(alias="endDate")
    lotno: str = Field(alias="lotNo")
    productid: str = Field(alias="productId")
    quantity: int
    createdby: Optional[str] = Field(default=None, alias="createdBy")
    # createddate: Optional[datetime] = Field(default_factory=datetime.now, alias="createdDate")

class TransactionUpdate(BaseModel):
    startdate: Optional[datetime] = Field(default=None, alias="startDate")
    enddate: Optional[datetime] = Field(default=None, alias="endDate")
    lotno: Optional[str] = Field(default=None, alias="lotNo")
    productid: Optional[str] = Field(default=None, alias="productId")
    quantity: Optional[int] = None
    updatedby: Optional[str] = Field(default=None, alias="updatedBy")
    # updateddate: Optional[datetime] = Field(default_factory=datetime.now, alias="updatedDate")

class ReportDefectCreate(BaseModel):
    lotno: str = Field(alias="lotNo")
    producttype: str = Field(alias="productType")
    defecttype: str = Field(alias="defectType")
    total: int
    ok: int
    ng: int

class ReportDefectUpdate(BaseModel):
    producttype: Optional[str] = Field(default=None, alias="productType")
    defecttype: Optional[str] = Field(default=None, alias="defectType")
    total: Optional[int] = None
    ok: Optional[int] = None
    ng: Optional[int] = None

class ReportProductCreate(BaseModel):
    datetime: datetime
    productid: str = Field(alias="productId")
    productname: str = Field(alias="productName")
    lotno: str = Field(alias="lotNo")
    status: str
    defecttype: str = Field(alias="defectType")
    cameraid: str = Field(alias="cameraId")

class ReportProductUpdate(BaseModel):
    status: Optional[str] = None
    defecttype: Optional[str] = Field(default=None, alias="defectType")
    cameraid: Optional[str] = Field(default=None, alias="cameraId")

class HistoryItem(BaseModel):
    date: str
    time: str
    updatedby: str = Field(alias="updatedBy")

class ProductDetailCreate(BaseModel):
    productid: str = Field(alias="productId")
    productname: str = Field(alias="productName")
    serialno: str = Field(alias="serialNo")
    date: str
    time: str
    lotno: str = Field(alias="lotNo")
    defecttype: str = Field(alias="defectType")
    cameraid: str = Field(alias="cameraId")
    status: str
    comment: str
    history: List[HistoryItem]

class PermissionCreate(BaseModel):
    permissionid: int = Field(alias="permissionId")
    menuid: str = Field(alias="menuId")
    actionid: int = Field(alias="actionId")

class PermissionUpdate(BaseModel):
    menuid: Optional[str] = Field(default=None, alias="menuId")
    actionid: Optional[int] = Field(default=None, alias="actionId")

class MenuCreate(BaseModel):
    menuid: str = Field(alias="menuId")
    parentid: Optional[str] = Field(default=None, alias="parentId")
    menuname: str = Field(alias="menuName")
    icon: Optional[str] = Field(default=None, alias="icon")
    seq: int
    path: str

class MenuUpdate(BaseModel):
    parentid: Optional[str] = Field(default=None, alias="parentId")
    menuname: Optional[str] = Field(default=None, alias="menuName")
    icon: Optional[str] = Field(default=None, alias="icon")
    seq: Optional[int] = None
    path: Optional[str] = None

class DashboardFilter(BaseModel):
    start: datetime = Field(alias="startDate")
    end: datetime = Field(alias="endDate")
    productname: Optional[str] = Field(default=None, alias="productName")
    prodline: Optional[str] = Field(default=None, alias="lineNo")
    cameraid: Optional[str] = Field(default=None, alias="cameraId")

class Config:
    orm_mode = True
    allow_population_by_field_name = True
