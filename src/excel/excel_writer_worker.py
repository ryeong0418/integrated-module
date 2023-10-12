from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import Workbook
from openpyxl.styles import Alignment


class ExcelAlignment:
    """
    ExcelAlignment class
    """

    ALIGN_VERTICAL = "align_vertical"
    ALIGN_WRAP_TEXT = "align_wrap_text"


class ExcelWriterWorker:
    """
    ExcelWriterWorker class
    """

    def __init__(self):
        self.wb = None
        self.active_worksheet = None

    def create_workbook(self):
        """
        workbook 생성 및 default sheet를 active_worksheet 설정
        """
        self.wb = Workbook()
        self.active_worksheet = self.wb.active

    def set_active_sheet_name(self, sheet_name: str):
        """
        active_worksheet에 title 설정 함수 (10자리 제한)
        :param sheet_name: 시트 title 이름
        :return: (10자리 제한된) 시트 title 이름
        """
        if len(sheet_name) > 10:
            sheet_name = sheet_name[:10]

        self.active_worksheet.title = sheet_name
        return sheet_name

    def set_cell_width_columns(self, columns_width_dict: dict):
        """
        active_worksheet에 특정 컬럼의 width 설정 함수
        :param columns_width_dict: 컬럼 width dict
        """
        for column, width in columns_width_dict.items():
            self.active_worksheet.column_dimensions[column].width = width

    def autofit_column_size(self, columns=None, margin=2):
        """
        컬럼 자동 크기 조정 함수
        :param columns: column list
        :param margin: margin value
        """
        for i, column_cells in enumerate(self.active_worksheet.columns):
            is_ok = False
            if columns is None:
                is_ok = True
            elif isinstance(columns, list) and i in columns:
                is_ok = True

            if is_ok:
                length = max(len(str(cell.value)) for cell in column_cells)
                self.active_worksheet.column_dimensions[column_cells[0].column_letter].width = length + margin

    def set_value_from_pandas(
        self, df, start_row_index, align_vertical=False, contain_index=False, contain_header=False
    ):
        """
        active_worksheet에 데이터프레임의 값을 넣는 함수
        :param df: 출력 하려는 데이터프레임
        :param start_row_index: 시작 row index
        :param align_vertical: align vertical flag
        :param contain_index: index 포함 flag
        :param contain_header: header 포함 flag
        """
        for r in dataframe_to_rows(df, index=contain_index, header=contain_header):
            self.active_worksheet.append(r)

        if align_vertical:
            self._set_style_by_option(ExcelAlignment.ALIGN_VERTICAL, start_row_index, df)

    def set_style_by_option(self, df, start_row_index, style_option_dict: dict):
        """
        style 설정 함수
        :param df: style 설정 하려는 데이터 프레임
        :param start_row_index: 시작 row index
        :param style_option_dict: style 옵션 (ExcelAlignment class 사용)
        """
        for column_value, style in style_option_dict.items():
            self._set_style_by_option(ExcelAlignment.ALIGN_WRAP_TEXT, start_row_index, df, column_value)

    # 현재 사용하는 style은 alignment에 대해서만 적용
    def _set_style_by_option(self, alignment_style, start_row_index, df, column_to_style=None):
        """
        실제 style 설정 함수
        :param alignment_style: alignment style
        :param start_row_index: 시작 row index
        :param df: style 적용 데이터 프레임
        :param column_to_style: style 적용하려는 column
        """
        min_col = None
        max_col = None

        if alignment_style == ExcelAlignment.ALIGN_VERTICAL:
            alignment_style = Alignment(vertical="center")

        elif alignment_style == ExcelAlignment.ALIGN_WRAP_TEXT:
            alignment_style = Alignment(wrap_text=True)

            min_col = df.columns.get_loc(column_to_style) + 1
            max_col = df.columns.get_loc(column_to_style) + 1
        else:
            alignment_style = Alignment(vertical="top")

        for cell in self.active_worksheet.iter_rows(
            min_row=start_row_index + 1, max_row=start_row_index + len(df) + 1, min_col=min_col, max_col=max_col
        ):
            for cell_in_row in cell:
                cell_in_row.alignment = alignment_style

    def set_height_row(self, row, height):
        """
        active_worksheet row에 height 설정 함수
        :param row: row index
        :param height: height 설정값
        """
        self.active_worksheet.row_dimensions[row].height = height

    def set_freeze_panes(self, cell_value):
        """
        active_worksheet 셀 고정 설정 함수
        :param cell_value: cell value
        """
        self.active_worksheet.freeze_panes = cell_value

    def save_workbook(self, file_path, file_name):
        """
        workbook 저장 함수
        :param file_path: file path
        :param file_name: file name
        """
        self.wb.save(f"{file_path}/{file_name}")
        self.wb.close()
