import datetime
import tempfile
import os
import time

import pandas
import psycopg2
from peewee import PostgresqlDatabase, Model, CharField, ForeignKeyField, DateField, DecimalField, IntegerField
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.action_chains import ActionChains


DB_CONF = {
    'host': os.environ['db_host'],
    'name': os.environ['db_name'],
    'user': os.environ['db_user'],
    'password': os.environ['db_password'],
    'port': os.environ['db_port']
}


pg_database = PostgresqlDatabase(
    DB_CONF["name"],
    user=DB_CONF["user"],
    password=DB_CONF["password"],
    host=DB_CONF["host"],
    port=DB_CONF["port"],
)


class BaseModel(Model):
    class Meta:
        database = pg_database


class ShoppersStopBrand(BaseModel):
    brand_name = CharField(null=False)
    url = CharField()
    user_name = CharField()
    password = CharField()


class ShoppersStopSale(BaseModel):
    partner = CharField()
    brand = ForeignKeyField(ShoppersStopBrand)
    location = CharField()
    distribution_channel = CharField()
    style_code = CharField()
    style_desc = CharField()
    hsn_code = CharField()
    sku_no = CharField()
    barcode_scanned_or_billing = CharField()
    gs1_number = CharField()
    selling_date = DateField()
    brand_name = CharField()
    category = CharField()
    sub_category = CharField()
    partner_part_no = CharField()
    colour_desc = CharField()
    size_desc = CharField()
    qty_sold = IntegerField()
    actual_retail_value = DecimalField()
    discount_amount = DecimalField()
    net_retail_value = DecimalField()
    posting_date = DateField()


def lambda_handler(event, context):
    print("Started at:", datetime.datetime.now())
    download_path = tempfile.gettempdir()

    print("Temp path:", _path)

    all_brands = ShoppersStopBrand.select()

    # Iterate over the rows and access the column values
    for brand in all_brands:
        url = brand.url
        user_name = brand.user_name
        password = brand.password
        chrome_options = Options()
        chrome_options.add_experimental_option("detach", True)
        p = {"download.default_directory": download_path}
        chrome_options.add_experimental_option("prefs", p)
        # Configure the Selenium driver (replace with the appropriate driver for your browser)
        driver = webdriver.Chrome(options=chrome_options)

        # Open the website
        driver.get(url=url)
        WebDriverWait(driver=driver, timeout=3)
        print("Driver is running")
        close_btn = driver.find_element(By.XPATH, '//a[@class="close"]')

        close_btn.click()
        print("Close button clicked")

        username_input = driver.find_element(By.ID, "txtUserId")
        for ch in user_name:
            username_input.send_keys(ch)

        password_input = driver.find_element(By.ID, "txtPassword")
        for ch in password:
            password_input.send_keys(ch)

        login_button = driver.find_element(By.ID, "irbLoginButton")
        login_button.click()
        print("Login button clicked")

        # Wait for the page to load after login (replace 'button_locator' with the locator of your target button)

        report_btn = WebDriverWait(driver=driver, timeout=150).until(
            expected_conditions.element_to_be_clickable(
                (By.XPATH, '//*[@href="/SitePages/Reports%20Home_Org.aspx"]')
            )
        )
        print(report_btn.is_enabled())
        report_btn.click()
        print("Report button clicked")

        sales_details_button = WebDriverWait(driver=driver, timeout=300).until(
            expected_conditions.element_to_be_clickable(
                (By.XPATH, '//*[@href="/Report%20Library/SalesDetails.rdl"]')
            )
        )

        print(sales_details_button.is_enabled())
        sales_details_button.click()
        WebDriverWait(driver=driver, timeout=30)

        print("Sales button clicked")

        today_date = datetime.date.today()
        start_date = today_date - datetime.timedelta(days=1)
        to_date = start_date
        print("Exporting data from {} to {}".format(start_date, to_date))
        
        from_date_button = WebDriverWait(driver=driver, timeout=120).until(
            expected_conditions.presence_of_element_located(
                (By.XPATH, '//*[@id="m_sqlRsWebPart_ctl00_ctl19_ctl06_ctl03_txtValue"]')
            )
        )
        from_date_button.clear()
        start_date = str(start_date.strftime("%m/%d/%Y"))
        for ch in start_date:
            from_date_button.send_keys(ch)

        to_date_button = driver.find_element(
            By.XPATH, '//*[@id="m_sqlRsWebPart_ctl00_ctl19_ctl06_ctl05_txtValue"]'
        )
        to_date_button.clear()
        to_date = str(to_date.strftime("%m/%d/%Y"))
        for ch in to_date:
            to_date_button.send_keys(ch)
        
        partner_select = Select(
            driver.find_element(By.ID, "m_sqlRsWebPart_ctl00_ctl19_ctl06_ctl07_ddValue")
        )
        partner_select.select_by_visible_text(
            "0001300777 () -GBL HOME AND KITCHEN PVT LTD -HR"
        )

        WebDriverWait(driver=driver, timeout=90)

        apply_button = WebDriverWait(driver=driver, timeout=100).until(
            expected_conditions.element_to_be_clickable(
                (By.XPATH, '//*[@id="m_sqlRsWebPart_ctl00_ctl19_ApplyParameters"]')
            )
        )
        apply_button.click()
        print("Apply button clicked")

        action_button = WebDriverWait(driver=driver, timeout=160).until(
            expected_conditions.element_to_be_clickable(
                (
                    By.ID,
                    "m_sqlRsWebPart_RSWebPartToolbar_ctl00_RptControls_RSActionMenu_ctl01_t",
                )
            )
        )

        actions = ActionChains(driver)
        actions.click(action_button).pause(5).perform()
        print("Action button clicked")
        export_button = WebDriverWait(driver, 10).until(
            expected_conditions.presence_of_element_located(
                (By.XPATH, '//*[@id="mp1_0_2_Anchor"]')
            )
        )
        actions.click(export_button).pause(5).perform()
        download_button = driver.find_element(By.XPATH, '//*[@id="mp1_1_4_Anchor"]')

        download_button.click()
        print("Download button is  clicked")
        seconds = 1
        comeout = True
        while True and comeout:
            if os.path.isfile((os.path.join(download_path, "SalesDetails.xlsx"))):
                print("file is downloaded")
                break
            else:
                time.sleep(1)
                seconds = seconds + 1
                if seconds > 300:
                    comeout = False

        driver.quit()

        path_to_file = os.path.join(download_path, "SalesDetails.xlsx")

        df = pandas.read_excel(path_to_file, skiprows=4)

        df = df.iloc[:-1]
        df = df.iloc[:, 1:]
        print("Reading the data")
        sales_data = []
        for _, row in df.iterrows():
            sales_data.append(
                {
                    "partner": row[0],
                    "brand": brand.id,
                    "location": row[1],
                    "distribution_channel": row[2],
                    "style_code": row[3],
                    "style_desc": row[4],
                    "hsn_code": row[5],
                    "sku_no": row[6],
                    "barcode_scanned_or_billing": row[7],
                    "gs1_number": row[8],
                    "selling_date": row[9],
                    "brand_name": row[10],
                    "category": row[11],
                    "sub_category": row[12],
                    "partner_part_no": row[13],
                    "colour_desc": row[14],
                    "size_desc": row[15],
                    "qty_sold": row[16],
                    "actual_retail_value": row[17],
                    "discount_amount": row[18],
                    "net_retail_value": row[19],
                    "posting_date": row[20],
                }
            )

        ShoppersStopSale.insert_many(sales_data).execute()

        print("program is completed and Data has been pushed to database")
        print("Ended at:", datetime.datetime.now())
