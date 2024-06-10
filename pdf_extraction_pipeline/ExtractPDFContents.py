import os

import datetime as dt
import pymupdf  as ppdf

class ExtractPDFContents:
    def __init__(self):
        self.current_date = str(dt.datetime.now().date())

    def pdf_extraction_settings_method(self, incross_media_pdf_path: str, mezzo_media_pdf_path: str, nas_media_pdf_path: str, incross_extraction_path: str, mezzo_extraction_path: str, nas_extraction_path: str):
        self.incross_media_pdf_path  = incross_media_pdf_path
        self.mezzo_media_pdf_path    = mezzo_media_pdf_path
        self.nas_media_pdf_path      = nas_media_pdf_path
        self.incross_extraction_path = incross_extraction_path
        self.mezzo_extraction_path   = mezzo_extraction_path 
        self.nas_extraction_path     = nas_extraction_path

        for output_path in [incross_extraction_path, mezzo_extraction_path, nas_extraction_path]:
            os.makedirs(output_path, exist_ok = True)
        
    def extract_pdf_file_contents(self):
        def extract_pdf_contents(pdf_files_list: list[str]):
            for file_name in pdf_files_list:
                document_object = ppdf.open(file_name)


        input_paths_list  = [self.incross_media_pdf_path, self.mezzo_media_pdf_path, self.nas_media_pdf_path]
        output_paths_list = [self.incross_extraction_path, self.mezzo_extraction_path, self.nas_extraction_path]

        for (input_path, output_path) in zip(input_paths_list, output_paths_list):
            pdf_files_list  = os.listdir(input_path.format(self.current_date))
            
