from typing import Annotated, Final, Self

from gspread import service_account
from gspread.worksheet import Worksheet
from pydantic import BaseModel, ConfigDict

from app import config
from app.paths import ROOT_PATH

from ..shared.decorators import retry_on_fail
from .enums import CheckType
from .exceptions import SheetError
from .g_sheet import gsheet_client

COL_META: Final[str] = "col_name_xxx"
IS_UPDATE_META: Final[str] = "is_update_xxx"
IS_NOTE_META: Final[str] = "is_note_xxx"


class ColSheetModel(BaseModel):
    # Model config
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sheet_id: str
    sheet_name: str
    index: int

    @classmethod
    def get_worksheet(
        cls,
        sheet_id: str,
        sheet_name: str,
    ) -> Worksheet:
        spreadsheet = gsheet_client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)

        return worksheet

    @classmethod
    def mapping_fields(cls) -> dict:
        mapping_fields = {}
        for field_name, field_info in cls.model_fields.items():
            if hasattr(field_info, "metadata"):
                for metadata in field_info.metadata:
                    if COL_META in metadata:
                        mapping_fields[field_name] = metadata[COL_META]
                        break

        return mapping_fields

    @classmethod
    def update_mapping_fields(cls) -> dict:
        mapping_fields = {}
        for field_name, field_info in cls.model_fields.items():
            if hasattr(field_info, "metadata"):
                for metadata in field_info.metadata:
                    if COL_META in metadata and IS_UPDATE_META in metadata:
                        mapping_fields[field_name] = metadata[COL_META]
                        break

        return mapping_fields

    @classmethod
    def get(
        cls,
        sheet_id: str,
        sheet_name: str,
        index: int,
    ) -> Self:
        mapping_dict = cls.mapping_fields()

        query_value = []

        for _, v in mapping_dict.items():
            query_value.append(f"{v}{index}")

        worksheet = cls.get_worksheet(sheet_id=sheet_id, sheet_name=sheet_name)

        model_dict = {
            "index": index,
            "sheet_id": sheet_id,
            "sheet_name": sheet_name,
        }

        query_results = worksheet.batch_get(query_value)
        count = 0
        for k, _ in mapping_dict.items():
            model_dict[k] = query_results[count].first()
            if isinstance(model_dict[k], str):
                model_dict[k] = model_dict[k].strip()
            count += 1
        return cls.model_validate(model_dict)

    @classmethod
    def batch_get(
        cls,
        sheet_id: str,
        sheet_name: str,
        indexes: list[int],
    ) -> list[Self]:
        worksheet = cls.get_worksheet(
            sheet_id=sheet_id,
            sheet_name=sheet_name,
        )
        mapping_dict = cls.mapping_fields()

        result_list: list[Self] = []

        query_value = []
        for index in indexes:
            for _, v in mapping_dict.items():
                query_value.append(f"{v}{index}")

        query_results = worksheet.batch_get(query_value)

        count = 0

        for index in indexes:
            model_dict = {
                "index": index,
                "sheet_id": sheet_id,
                "sheet_name": sheet_name,
            }

            for k, _ in mapping_dict.items():
                model_dict[k] = query_results[count].first()
                if isinstance(model_dict[k], str):
                    model_dict[k] = model_dict[k].strip()
                count += 1

            result_list.append(cls.model_validate(model_dict))
        return result_list

    @classmethod
    @retry_on_fail(max_retries=3, sleep_interval=30)
    def batch_update(
        cls,
        sheet_id: str,
        sheet_name: str,
        list_object: list[Self],
    ) -> None:
        worksheet = cls.get_worksheet(
            sheet_id=sheet_id,
            sheet_name=sheet_name,
        )
        mapping_dict = cls.update_mapping_fields()
        update_batch = []

        for object in list_object:
            model_dict = object.model_dump(mode="json")

            for k, v in mapping_dict.items():
                update_batch.append(
                    {
                        "range": f"{v}{object.index}",
                        "values": [[model_dict[k]]],
                    }
                )

        if len(list_object) > 0:
            worksheet.batch_update(update_batch)

    @retry_on_fail(max_retries=3, sleep_interval=30)
    def update(
        self,
    ) -> None:
        mapping_dict = self.update_mapping_fields()
        model_dict = self.model_dump(mode="json")

        worksheet = self.get_worksheet(
            sheet_id=self.sheet_id, sheet_name=self.sheet_name
        )

        update_batch = []
        for k, v in mapping_dict.items():
            update_batch.append(
                {
                    "range": f"{v}{self.index}",
                    "values": [[model_dict[k]]],
                }
            )

        worksheet.batch_update(update_batch)

    @classmethod
    @retry_on_fail(max_retries=5, sleep_interval=30)
    def update_note_message(
        cls,
        sheet_id: str,
        sheet_name: str,
        index: int,
        messages: str,
    ):
        for field_name, field_info in cls.model_fields.items():
            if hasattr(field_info, "metadata"):
                for metadata in field_info.metadata:
                    if COL_META in metadata and IS_NOTE_META in metadata:
                        worksheet = cls.get_worksheet(
                            sheet_id=sheet_id,
                            sheet_name=sheet_name,
                        )

                        worksheet.batch_update(
                            [
                                {
                                    "range": f"{metadata[COL_META]}{index}",
                                    "values": [[messages]],
                                }
                            ]
                        )


class RowRun(ColSheetModel):
    CHECK: Annotated[
        str,
        {
            COL_META: "A",
        },
    ]
    PRODUCT_NAME: Annotated[
        str,
        {
            COL_META: "B",
        },
    ]
    PRODUCT_COMPARE: Annotated[
        str,
        {
            COL_META: "C",
        },
    ]
    LOWEST_PRICE_USD: Annotated[
        str | None,
        {
            COL_META: "D",
            IS_UPDATE_META: True,
        },
    ] = None
    LOWEST_PRICE_EUR: Annotated[
        str | None,
        {
            COL_META: "E",
            IS_UPDATE_META: True,
        },
    ] = None
    SELLER: Annotated[
        str | None,
        {
            COL_META: "F",
            IS_UPDATE_META: True,
        },
    ] = None
    Time_update: Annotated[
        str | None,
        {
            COL_META: "G",
            IS_UPDATE_META: True,
        },
    ] = None
    Note: Annotated[
        str | None,
        {
            COL_META: "H",
            IS_UPDATE_META: True,
            IS_NOTE_META: True,
        },
    ] = None
    Top: Annotated[
        str | None,
        {
            COL_META: "I",
            IS_UPDATE_META: True,
        },
    ] = None
    CNLGAMING_USD: Annotated[
        str | None,
        {
            COL_META: "J",
            IS_UPDATE_META: True,
        },
    ] = None
    CNLGAMING_EUR: Annotated[
        str | None,
        {
            COL_META: "K",
            IS_UPDATE_META: True,
        },
    ] = None
    FEEDBACK_QTY: Annotated[
        int,
        {
            COL_META: "L",
        },
    ]
    FEEDBACK_PERCENT: Annotated[
        float,
        {
            COL_META: "M",
        },
    ]
    DELIVERY_TIME: Annotated[
        int,
        {
            COL_META: "N",
        },
    ]
    MIN_QTY: Annotated[
        int,
        {
            COL_META: "O",
        },
    ]
    STOCK1: Annotated[
        int,
        {
            COL_META: "P",
        },
    ]
    BLACKLIST_RANGE: Annotated[
        str,
        {
            COL_META: "Q",
        },
    ]
    RELAX: Annotated[
        float,
        {
            COL_META: "R",
        },
    ]

    @staticmethod
    @retry_on_fail(max_retries=5, sleep_interval=10)
    def get_run_indexes(sheet_id: str, sheet_name: str, col_index: int) -> list[int]:
        sheet = RowRun.get_worksheet(sheet_id=sheet_id, sheet_name=sheet_name)
        run_indexes = []
        check_col = sheet.col_values(col_index)
        for idx, value in enumerate(check_col):
            idx += 1
            if not isinstance(value, str):
                value = str(value)
            if value in [type.value for type in CheckType]:
                run_indexes.append(idx)

        return run_indexes

    def get_blacklist(self) -> list[str]:
        g_client = service_account(ROOT_PATH.joinpath(config.KEYS_PATH))

        spreadsheet = g_client.open_by_key(self.sheet_id)

        worksheet = spreadsheet.worksheet(self.sheet_name)

        blacklist = worksheet.batch_get([self.BLACKLIST_RANGE])[0]
        if blacklist:
            res = []
            for blist in blacklist:
                for i in blist:
                    res.append(i)
            return res

        raise SheetError(
            f"{self.sheet_id}->{self.sheet_name}->{self.BLACKLIST_RANGE} is None"
        )
