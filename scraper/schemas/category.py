from pydantic import BaseModel

class CategoryCreate(BaseModel):
    category_code: str
    category_name: str

class CategoryResponse(CategoryCreate):
    id: int

    class Config:
        orm_mode = True
