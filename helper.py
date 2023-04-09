from bs4 import BeautifulSoup
import logging
import requests
import os
import zipfile
import pandas as pd
import boto3


def get_link_from_xml():
    """
    Extracts the first download link from doc having file_type as DLTINS,
    from the given xml file.
    :return:
        text (str) : Returns the link to target zip file
    """
    try:
        # Opening the xml file
        logging.info('Trying to open the xml file')
        with open('res/select.xml', 'r') as file:
            xml_data = file.read()

        if xml_data:
            logging.info('File loaded successfully')
        else:
            logging.error('Some error occurred, try again!!!')
            exit(-1)

        # Parsing the xml file with BeautifulSoup
        soup = BeautifulSoup(xml_data, 'xml')

        docs = soup.find_all('doc')

        logging.info('Traversing all the docs')
        for doc in docs:
            file_type = doc.find('str', {'name': 'file_type'}).text
            if file_type == 'DLTINS':
                logging.info('Found tag having file_type as DLTINS')
                logging.info('Extracting the download link')
                download_link = doc.find('str', {'name': 'download_link'}).text
                break
        else:
            logging.error('No tag found having file_type as DLTINS')
            exit(-1)

        return download_link

    except Exception as e:
        logging.error('Some error occured' + e)


def download_zip(file_url: str, download_path: str):
    """
    Downloading the zip file from the given link using the requests module
    :param file_url: Link to the zip file
    :param download_path: Path where the zip file will get downloaded
    :return:
        success (bool): Returns true when file get downloaded successfully
    """

    # Extracting the filename from the url
    filename = os.path.basename(file_url)
    logging.info('Filename: ' + filename)

    logging.info('Checking if zip file is already present')
    # Checking if zip file is already present
    if os.path.exists(os.path.join(download_path, filename)):
        logging.info(filename + 'already exist in directory: ' + download_path)
        return True

    else:
        try:
            # Sending a GET request to download the ZIP file
            response = requests.get(file_url)

            # Checking if request was successful
            if response.status_code == 200:
                logging.info('File downloaded successfully')
                with open(os.path.join(download_path, filename), 'wb') as file:
                    file.write(response.content)
                logging.info(filename + 'downloaded to directory: ' + download_path + ' successfully.')
                return True

            else:
                logging.error('Some error occurred, Status code'
                              + str(response.status_code) +
                              '\nexiting!!!')
                return False

        except Exception as e:
            logging.error('Failed to download' + filename)
            return False


def extracting_content_from_zip(file_path: str, extract_path: str):
    """
    Extracts contents of the zip file and saves them in extract_path
    :param file_path: Source ZIP file url
    :param extract_path: Path where extracted files should be saved
    :return:
        Success(bool): Whether extract operation succeeded or not
    """

    try:
        logging.info('Extracting the contents of the zip file to ' + extract_path)
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            zip_file.extractall(extract_path)

        logging.info(file_path + 'successfully extracted to ' + extract_path)
        return True

    except zipfile.BadZipfile as e:
        logging.error('Failed to extract zip file' + file_path)
        return False
    except Exception as e:
        logging.error('Some error occured while extracting zip file: ', e)
        return False


def xml_to_csv(xml_path: str, csv_path: str):
    """
    Converts the xml file to csv having specified column names
    :param xml_path: Path to XML file
    :param csv_path: Path where CSV file needs to be saved
    :return: csv_path
    """
    try:
        xml_name = os.path.basename(xml_path)

        # Extracting the CSV file name from XML file
        csv_name = os.path.splitext(xml_name)[0] + '.csv'

        # Required columns needed to be present inside the csv file
        csv_columns = [
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr",
        ]

        # Reading the xml file and storing data in xml_data
        with open(xml_path, 'r', encoding='utf-8') as file:
            xml_data = file.read()
        logging.info('Read the xml file successfully')

        # Parsing XML file with beautiful soup
        soup = BeautifulSoup(xml_data, 'xml')

        # Finding all FinInstrm tags inside soup
        FinInstrm = soup.find_all('FinInstrm')

        # List of data which will be storing dictonaries of items need to be inserted in the Dataframe
        data = list()

        for values in FinInstrm:
            temp_data = dict()

            # Checking if TermntdRcrd exists in FinInstrm
            if values.TermntdRcrd:

                # Checking if FinInstrmGnlAttrbts exists in TermntdRcrd
                if values.TermntdRcrd.FinInstrmGnlAttrbts:

                    # Checking if Id exists in FinInstrmGnlAttrbts
                    if values.TermntdRcrd.FinInstrmGnlAttrbts.Id:
                        temp_data[csv_columns[0]] = values.TermntdRcrd.FinInstrmGnlAttrbts.Id.text

                    # Checking if FullNm exists in FinInstrmGnlAttrbts
                    if values.TermntdRcrd.FinInstrmGnlAttrbts.FullNm:
                        temp_data[csv_columns[1]] = values.TermntdRcrd.FinInstrmGnlAttrbts.FullNm.text

                    # Checking if ClssfctnTp exists in FinInstrmGnlAttrbts
                    if values.TermntdRcrd.FinInstrmGnlAttrbts.ClssfctnTp:
                        temp_data[csv_columns[2]] = values.TermntdRcrd.FinInstrmGnlAttrbts.ClssfctnTp.text

                    # Checking if CmmdtyDerivInd exists in FinInstrmGnlAttrbts
                    if values.TermntdRcrd.FinInstrmGnlAttrbts.CmmdtyDerivInd:
                        temp_data[csv_columns[3]] = values.TermntdRcrd.FinInstrmGnlAttrbts.CmmdtyDerivInd.text

                    # Checking if NtnlCcy exists in FinInstrmGnlAttrbts
                    if values.TermntdRcrd.FinInstrmGnlAttrbts.NtnlCcy:
                        temp_data[csv_columns[4]] = values.TermntdRcrd.FinInstrmGnlAttrbts.NtnlCcy.text

                # Checking if Issr exists in TermntdRcrd
                if values.TermntdRcrd.Issr:
                    temp_data[csv_columns[5]] = values.TermntdRcrd.Issr.text

                # Inserting all the data into final list
                data.append(temp_data)

        logging.info('All the data extracted successfully from the XML file')

        logging.info('Creating pandas DataFrame with the data extracted from the xml file using the required columns')
        df = pd.DataFrame(data=data, columns=csv_columns)

        csv_path = os.path.join(csv_path, csv_name)

        logging.info('Converting the Pandas Dataframe and saving it')
        df.to_csv(csv_path, index=False)

        return csv_path

    except Exception as e:
        logging.error('Some error occurred while converting xml to csv:', e)


def aws_s3_upload(file: str, bucket_name: str, s3_key: str):
    """
    Uploads any file to s3 storage in AWS with specific bucket_name and s3_key
    :param file: Path to file, which needs to be uploaded to AWS S3
    :param bucket_name: Name of the bucket in S3
    :param s3_key: S3 Key
    :return:
        True(bool) : For successful upload
    """
    try:
        logging.info('Creating S3 resource')
        s3 = boto3.resource('s3')

        s3.Bucket(bucket_name).upload_file(Filename=file, Key=s3_key)
        logging.info('File uploaded to S3 bucket successfully')

    except Exception as e:
        logging.error('Some error occurred while uploading file to S3:', e)
