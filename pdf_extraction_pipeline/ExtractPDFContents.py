import os, json

import datetime as dt
import pymupdf  as ppdf

from konlpy.tag import Okt

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
        self.korean_text_tokenizer   = Okt()

        for output_path in [incross_extraction_path, mezzo_extraction_path, nas_extraction_path]:
            os.makedirs(output_path.format(self.current_date), exist_ok = True)
    
    def __extract_pagewise_contents(self, input_path: str, output_path: str, pdf_files_list: list[str]):
        def save_file_contents(output_path: str, file_name: str, file_contents: dict):
            for (page_number, document_page) in enumerate(document_object):
                page_text = document_page.get_text("text")
                page_text = ' '.join(self.korean_text_tokenizer.morphs(page_text))
                file_contents["file_contents"][f"Page_{page_number + 1}"] = page_text

            with open(os.path.join(output_path, file_name.replace(".pdf", ".json")), "w", encoding = "utf-8") as f:
                json.dump(file_contents, f, indent = 4, ensure_ascii = False)

        for file_name in pdf_files_list:
            document_object                = ppdf.open(os.path.join(input_path.format(self.current_date), file_name))
            file_contents                  = {}
            file_contents["file_name"]     = file_name
            file_contents["file_contents"] = {}
            save_file_contents(output_path.format(self.current_date), file_name, file_contents)            

    def extract_pdf_file_contents(self):
        input_paths_list  = [self.incross_media_pdf_path, self.mezzo_media_pdf_path, self.nas_media_pdf_path]
        output_paths_list = [self.incross_extraction_path, self.mezzo_extraction_path, self.nas_extraction_path]

        for (input_path, output_path) in zip(input_paths_list, output_paths_list):
            pdf_files_list = os.listdir(input_path.format(self.current_date))
            self.__extract_pagewise_contents(input_path, output_path, pdf_files_list)
    
if (__name__ == "__main__"):
    with open("./config/PDFDistributionConfig.json", "r", encoding = "utf-8") as f:
        config_dict = json.load(f)

    pdf_extractor = ExtractPDFContents()
    pdf_extractor.pdf_extraction_settings_method(**config_dict["PDFDistributionPipeline"]["pdf_extraction_settings_method"])
    pdf_extractor.extract_pdf_file_contents()
