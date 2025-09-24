import re
import pandas as pd

def read_txt_as_string(txt_file):
    with open(txt_file, "r", encoding="utf-8") as f:
        return f.read()
    
def capture_phone_number(all_text):
    pattern = re.compile(r'\(?(\d){3}\)?(\.|-)(\d){3}(\.|-)(\d){4}')
    matches = pattern.finditer(all_text)
    for match in matches:
        phone_number = re.sub(r'\D', "", match.group(0))  # remove all non-digits
    return phone_number
def capture_name(all_text):
    pattern = re.compile(r'^([a-zA-Z ])+\n')
    matches = pattern.finditer(all_text)
    for match in matches:
        name = match.group(0)
    return name

def capture_mail(all_text):
    pattern = re.compile(r'[a-zA-Z0-9_\.]+@[a-zA-Z0-9_]+\.(com|edu|net|gov|net)',re.IGNORECASE)
    matches = pattern.finditer(all_text)
    for match in matches:
        mail = match.group(0)
    return mail

def capture_experience(all_text):
    pattern = re.compile(r'Experience(.*?)(\.|,|\n)+(Education|Skills|Projects|$)', re.DOTALL | re.IGNORECASE)
    match = pattern.search(all_text)
    # print(match.group(0))
    if match:
        experience_text = match.group(1).strip()
        pattern = re.compile(r'\b\d{4}\b')  # match any 4-digit year
        matches = pattern.findall(experience_text)
        # print(matches)
    # Extract text
    experience_text = match.group(0).strip()
    print(experience_text)
    # Trim to 100 characters
    trimmed_text = experience_text[:100]

    return trimmed_text
    #     if matches:  # ✅ avoid min/max on empty list
    #         years = list(map(int, matches))
    #         l_min = min(years)
    #         l_max = max(years)
    #         print(l_max - l_min)
    #     else:
    #         print("No years found in experience section")
    # else:
    #     print("No experience section found")

def transformation(txt_file):
    all_text = read_txt_as_string(txt_file)
    capture_phone_number(all_text)
    
# def transform_text(str,engine):
#     phone_number = capture_phone_number(str)
#     mail = capture_mail(str)
#     name = capture_name(str)
#     experience = capture_experience(str)
#     df1 = pd.DataFrame({"name":name,"mail":mail,"experience":experience,"phone_number":phone_number})
#     df1.to_sql(name='resumes_data_table',con=engine,if_exists='append',index=False)

    
def transform_text(text, engine):
    phone_number = capture_phone_number(text)
    email = capture_mail(text)
    name = capture_name(text)
    experience = capture_experience(text)

    # Wrap scalars inside a list
    df1 = pd.DataFrame({
        "name": [name],
        "email": [email],
        "experience": [experience],
        "phone_number": [phone_number]
    })

    df1.to_sql(name='resumes_data_table', con=engine, if_exists='append', index=False)
