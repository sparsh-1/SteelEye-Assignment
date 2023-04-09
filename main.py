import helper
import os
import logging

if __name__ == '__main__':

    # Extracting link from the provided XML file
    link = helper.get_link_from_xml()

    if link:
        logging.info('Link extracted successfully from the XML file')

        logging.info('Starting download of ZIP file from web')
        if helper.download_zip(link, os.path.join(os.getcwd(), 'res\\')):

            logging.info('Staring Unzipping of the ZIP file to specified path')
            helper.extracting_content_from_zip(os.path.join(os.getcwd(), 'res\\DLTINS_20210117_01of01.zip'),
                                               os.path.join(os.getcwd(), 'res\\'))

            logging.info('Starting conversion of XML file to CSV with required columns')
            csv_path = helper.xml_to_csv(os.path.join(os.getcwd(), 'res\\DLTINS_20210117_01of01.xml'),
                              os.path.join(os.getcwd(), 'res\\'))

            if csv_path:
                logging.info('CSV file created and saved successfully to ' + csv_path)

                logging.info('Starting uploading CSV file to S3 bucket')
                helper.aws_s3_upload(csv_path, "put_bucket_name_here", "put_s3_key_here")

            else:
                logging.error('Some error occurred, Exiting!!!')
                exit(-1)

    else:
        logging.error('Some error occurred, try again!!!')

