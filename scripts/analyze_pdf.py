import pdfplumber

def extract_pdf_text(pdf_path, pages=None):
    with pdfplumber.open(pdf_path) as pdf:
        # User mentioned tables start on page 8 (index 7)
        # We'll scan a range to find the definitions
        start_page = 7 
        end_page = 20
        
        for i in range(start_page, min(end_page, len(pdf.pages))):
            page = pdf.pages[i]
            text = page.extract_text()
            print(f"--- Page {i+1} ---")
            print(text)
            print("-" * 50)

if __name__ == "__main__":
    extract_pdf_text("/mnt/e/Data Eng Exercise/DE 1.0 Codebook.pdf")
