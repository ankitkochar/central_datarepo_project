import os
import logging
import pdfplumber
from PIL import Image
import io
import pandas as pd
from tabulate import tabulate
import gc
import tempfile
import pytesseract
import logging

log_files_folder = os.environ.get("LOG_FILES_FOLDER")

try:
    log_file_path = os.path.join(log_files_folder, "download.log")
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s",
    )
    logging.info("Logging started successfully.")
except Exception as e:
    print(f"Failed to set up logging: {e}")


def perform_ocr_on_image(image_path, ocr):
    result = ocr.ocr(image_path, cls=True)
    text = ""
    for line in result[0]:
        if (
            isinstance(line, list)
            and len(line) > 1
            and isinstance(line[1], tuple)
            and isinstance(line[1][0], str)
        ):
            text += line[1][0] + " "
    return text.strip()


def is_block_within_table(block_bbox, table_bbox):
    x0, y0, x1, y1 = block_bbox
    tx0, ty0, tx1, ty1 = table_bbox
    return x0 < tx1 and x1 > tx0 and y0 < ty1 and y1 > ty0


def convert_pdf_to_markdown_using_paddleocr(file_path, actual_url):
    from paddleocr import PaddleOCR

    full_markdown = ""
    ocr = PaddleOCR(use_angle_cls=True, lang="en", show_log=False)

    with pdfplumber.open(io.BytesIO(file_path)) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            found_text = True
            ocr_text = ""
            tables = page.extract_tables()
            table_bboxes = [table.bbox for table in page.find_tables()]
            printed_tables = set()
            blocks = page.extract_words(use_text_flow=True, keep_blank_chars=True)
            images = page.images
            all_elements = blocks + images

            all_elements.sort(key=lambda b: b["top"] if "top" in b else b["y0"])

            current_y = 0
            line_text = ""
            min_width = max(
                150, page.width * 0.3
            )  
            min_height = max(
                50, page.height * 0.1
            ) 

            for element in all_elements:
                if "text" in element:  # Text blocks
                    block_bbox = (
                        element["x0"],
                        element["top"],
                        element["x1"],
                        element["bottom"],
                    )
                    block_text = element["text"].strip()

                    if element["top"] > current_y + 2:
                        if line_text:
                            full_markdown += line_text.strip() + "\n"
                            found_text = True
                            line_text = ""
                        current_y = element["top"]

                    is_within_table = False
                    for idx, table_bbox in enumerate(table_bboxes):
                        if is_block_within_table(block_bbox, table_bbox):
                            is_within_table = True
                            if idx not in printed_tables:
                                if line_text:
                                    full_markdown += line_text.strip() + "\n\n"
                                    line_text = ""
                                df = pd.DataFrame(
                                    tables[idx][1:], columns=tables[idx][0]
                                )
                                markdown_table = tabulate(
                                    df, headers="keys", tablefmt="pipe", showindex=False
                                )
                                full_markdown += (
                                    f"[TABLE]\n{markdown_table}\n[/TABLE]\n\n"
                                )
                                found_text = True
                                printed_tables.add(idx)
                            break

                    if not is_within_table:
                        line_text += block_text + " "
                        
                        
                elif "width" in element and "height" in element:  # Image blocks
                    x0, y0, x1, y1 = (
                        element["x0"],
                        element["top"],
                        element["x1"],
                        element["bottom"],
                    )
                    width, height = element["width"], element["height"]
                    page_bbox = (0, 0.0, page.width, page.height)
                    if width < min_width or height < min_height:
                        continue

                    if (
                        x0 >= page_bbox[0]
                        and x1 <= page_bbox[2]
                        and y0 >= page_bbox[1]
                        and y1 <= page_bbox[3]
                    ):
                        if line_text:
                            full_markdown += line_text.strip() + "\n\n"
                            line_text = ""

                        try:
                            cropped_image = (
                                page.within_bbox((x0, y0, x1, y1))
                                .to_image(resolution=300)
                                .original
                            )
                            img_byte_arr = io.BytesIO()
                            cropped_image.save(img_byte_arr, format="PNG")
                            img_byte_arr = img_byte_arr.getvalue()

                            with tempfile.NamedTemporaryFile(
                                delete=False, suffix=".png"
                            ) as temp_file:
                                temp_file.write(img_byte_arr)
                                ocr_text = (
                                    perform_ocr_on_image(temp_file.name, ocr) or ""
                                )
                                ocr_text = ocr_text.strip()
                                if ocr_text.strip():
                                    full_markdown += f"{ocr_text}\n"
                                    found_text = True
                            os.unlink(temp_file.name)
                        except Exception as e:
                            logging.error(
                                f"Error processing PADDLE OCR PDF:- {actual_url} on Page Number: {page_num} image data for OCR: {e}"
                            )

            if (not found_text) and images:
                try:
                    page_image = page.to_image(resolution=300).original
                    img_byte_arr = io.BytesIO()
                    page_image.save(img_byte_arr, format="PNG")
                    img_byte_arr = img_byte_arr.getvalue()

                    with tempfile.NamedTemporaryFile(
                        delete=False, suffix=".png"
                    ) as temp_file:
                        temp_file.write(img_byte_arr)
                        ocr_text_full_page = (
                            perform_ocr_on_image(temp_file.name, ocr) or ""
                        )
                        full_markdown += f"{ocr_text_full_page}\n"
                    os.unlink(temp_file.name)
                except Exception as e:
                    logging.error(
                        f"Error performing PADDLE OCR for PDF {actual_url} on Page Number:- {page_num} for the entire page: {e}"
                    )

            if line_text:
                full_markdown += line_text.strip()

            page.flush_cache()
            page.get_textmap.cache_clear()
            page.close()
            del tables, table_bboxes, printed_tables, blocks, all_elements, images
            gc.collect()

    return full_markdown


def convert_pdf_to_markdown_using_pytesseract(file_path, actual_url):
    full_markdown = ""

    with pdfplumber.open(io.BytesIO(file_path)) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            found_text = False
            ocr_text = ""
            tables = page.extract_tables()
            table_bboxes = [table.bbox for table in page.find_tables()]
            printed_tables = set()
            blocks = page.extract_words(use_text_flow=True, keep_blank_chars=True)
            images = page.images
            all_elements = blocks + images

            all_elements.sort(key=lambda b: b["top"] if "top" in b else b["y0"])

            current_y = 0
            line_text = ""
            min_width = max(150, page.width * 0.3)
            min_height = max(50, page.height * 0.1)
            for element in all_elements:
                if "text" in element:  # Text blocks
                    block_bbox = (
                        element["x0"],
                        element["top"],
                        element["x1"],
                        element["bottom"],
                    )
                    block_text = element["text"].strip()

                    if element["top"] > current_y + 2:
                        if line_text:
                            full_markdown += line_text.strip() + "\n"
                            line_text = ""
                        current_y = element["top"]

                    is_within_table = False
                    for idx, table_bbox in enumerate(table_bboxes):
                        if is_block_within_table(block_bbox, table_bbox):
                            is_within_table = True
                            if idx not in printed_tables:
                                if line_text:
                                    full_markdown += line_text.strip() + "\n\n"
                                    line_text = ""
                                df = pd.DataFrame(
                                    tables[idx][1:], columns=tables[idx][0]
                                )
                                markdown_table = tabulate(
                                    df, headers="keys", tablefmt="pipe", showindex=False
                                )
                                full_markdown += (
                                    f"[TABLE]\n{markdown_table}\n[/TABLE]\n\n"
                                )
                                printed_tables.add(idx)
                                found_text = True
                            break

                    if not is_within_table and block_text:
                        line_text += block_text + " "
                        found_text = True
                elif "width" in element and "height" in element:  # Image blocks
                    x0, y0, x1, y1 = (
                        element["x0"],
                        element["top"],
                        element["x1"],
                        element["bottom"],
                    )
                    width, height = element["width"], element["height"]
                    page_bbox = (0, 0.0, page.width, page.height)
                    if width < min_width or height < min_height:
                        continue

                    if (
                        x0 >= page_bbox[0]
                        and x1 <= page_bbox[2]
                        and y0 >= page_bbox[1]
                        and y1 <= page_bbox[3]
                    ):
                        if line_text:
                            full_markdown += line_text.strip() + "\n\n"
                            line_text = ""

                        try:
                            cropped_image = (
                                page.within_bbox((x0, y0, x1, y1))
                                .to_image(resolution=300)
                                .original
                            )

                            img_byte_arr = io.BytesIO()
                            cropped_image.save(img_byte_arr, format="PNG")
                            img_byte_arr = img_byte_arr.getvalue()
                            img = Image.open(io.BytesIO(img_byte_arr))
                            ocr_text = pytesseract.image_to_string(img) or ""
                            if len(ocr_text) > 0:
                                found_text = True
                            # print(f"ocr occured image-wise {page_num}")
                            ocr_text = ocr_text.strip()
                            # print(f"ocr text image-wise {ocr_text}")
                            if ocr_text.strip():
                                full_markdown += f" {ocr_text} "
                        except Exception as e:
                            logging.error(
                                f"Error processing PYTESSERACT OCR PDF:-  Page Number: {page_num} image data for OCR: {e}"
                            )

            if (not found_text) and images:
                try:
                    # print(f"ocr occuring as a full page {page_num}")
                    page_image = page.to_image(resolution=300).original
                    ocr_text_full_page = pytesseract.image_to_string(page_image) or ""
                    full_markdown += f" {ocr_text_full_page} "
                    # print(f"ocr text page-wise {ocr_text_full_page}")
                except Exception as e:
                    logging.error(
                        f"Error performing PYTESSERACT OCR for PDF  on Page Number:- {page_num} for the entire page: {e}"
                    )

            if line_text:
                full_markdown += line_text.strip()
            page.flush_cache()
            page.get_textmap.cache_clear()
            page.close()
            del tables, table_bboxes, printed_tables, blocks, all_elements, images
            gc.collect()

    return full_markdown
