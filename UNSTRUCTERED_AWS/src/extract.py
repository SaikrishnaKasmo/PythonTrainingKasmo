from PyPDF2 import PdfReader

def extract_text_from_pdf(pdf_file):
    with open(pdf_file,'rb') as pdf:
        reader = PdfReader(pdf)
        pdf_text = []
        for page in reader.pages:
            content = page.extract_text()
            if content:   # avoid None pages
                pdf_text.append(content)
        return pdf_text
        
