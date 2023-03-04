import os
import requests
import json 
import pkce
import pprint
import logging
import pandas as pd 
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from azure.storage.blob import BlobServiceClient, ContentSettings


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

format = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s  ')
file_handler = logging.FileHandler('cb_migration_tool.log', mode='w')
file_handler.setFormatter(format)
logger.addHandler(file_handler)

load_dotenv()


class XeroBot():
    redirect_uri="https://developer.xero.com/" 
    response_type = "code"
    code_challenge_method = "S256"
    state = "123"
    exchange_url = 'https://identity.xero.com/connect/token'
    connection_url="https://api.xero.com/connections"
    repeating_invoices_url = 'https://api.xero.com/api.xro/2.0/RepeatingInvoices'
    contacts_url = 'https://api.xero.com/api.xro/2.0/Contacts'
    refresh_token_url = "https://identity.xero.com/connect/token?="
    client_id=""
    authorization_url = ""
    authorization_code=""
    code_challenge=""
    code_verifier=""
    scope=""
    access_token=""
    refresh_token=""
    new_access_token=""
    new_refresh_token=""
    tenant_id=""
    repeating_invoices_json={}
    contacts_json={}

    

    def add_client_id(self, client_id):
        
        logger.debug(f'Now adding client ID of Xero App...')
        try:
            self.client_id=client_id
            logger.info(f'Successfully added client ID')
        except Exception as e:
            print(e)
            logger.error('Unable to add client ID - see traceback for more info')
        
        return self.client_id



    def add_scope(self, scope):
        logger.debug(f'Now adding scope for Xero Accounting API...')
        try:
            self.scope=scope
            logger.info(f'Successfully added scope')

        except Exception as e:
            print(e)
            logger.error('Unable to add scope - see traceback for more info')

        return scope
    


    def add_code_verifier(self):
        logger.debug(f'Now adding code verifier...')
        try:
            self.code_verifier = pkce.generate_code_verifier(length=128)
            logger.info(f'Successfully generated code verifier')
        except Exception as e:
            print(e)
            logger.error('Unable to add code verifier - see traceback for more info')

        return self.code_verifier



    def add_code_challenge(self):
        logger.debug(f'Now adding code challenge...')
        try:
            self.code_challenge=pkce.get_code_challenge(code_verifier=self.code_verifier)
            logger.info(f'Successfully generated code challenge')
        except Exception as e:
            print(e)
            logger.error('Unable to add code challenge - see traceback for more info')

        return self.code_challenge


    def set_up_web_automation_tool(self):
        logger.debug(f'Now setting up Xero web automation program...')

        chrome_driver = os.getenv("CHROME_DRIVER_PATH")
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        # options.add_argument("disable-infobars")    # browser will not show "Chrome is used by test automation software" message in infobar
        # options.add_argument('--headless')    # browser will not pop up while running

        chrome_driver = webdriver.Chrome(options=options)
        
        logger.info(f'Successfully completed the Xero web automation program setup')

        return chrome_driver



    def set_authorization_url(self) -> str:
        authorization_url = (
        '''
        https://login.xero.com/identity/connect/authorize?''' + 

        '''response_type=''' +  self.response_type +
        '''&code_challenge_method=''' + self.code_challenge_method + 
        '''&client_id=''' + self.client_id + 
        '''&scope=''' + self.scope +
        '''&redirect_uri=''' + self.redirect_uri +
        '''&state=''' + self.state +
        '''&code_challenge=''' + self.code_challenge
        )
        self.authorization_url=authorization_url

        return self.authorization_url



    def run_authorization_url(self):
            
        chrome_driver = XeroBot.set_up_web_automation_tool(self)
        authorization_url = XeroBot.set_authorization_url(self)


        # Load environment variables 
        
        xero_username = os.getenv('XERO_USERNAME')
        xero_password = os.getenv('XERO_PASSWORD')

        try:
            logger.debug(f'Now logging into Xero dev console...')
            chrome_driver.get(authorization_url)


            # Fill username and password credentials 

            add_username = chrome_driver.find_element(By.XPATH, '//*[@id="xl-form-email"]')
            add_username.click()
            add_username.send_keys(xero_username)

            add_password = chrome_driver.find_element(By.XPATH, '//*[@id="xl-form-password"]')
            add_password.click()
            add_password.send_keys(xero_password)

            submit_user_credentials = chrome_driver.find_element(By.XPATH, '//*[@id="xl-form-submit"]')
            submit_user_credentials.click()



            # Wait for 30 secs to authenticate automated login attempt via Xero Mobile app
            chrome_driver.implicitly_wait(30)

            logger.debug('Successfully logged into Xero dev console')

            
            # Select UK companies in dropdown menu
            
            logger.debug('Now selecting UK organisations ...')
            dropdown_menu = chrome_driver.find_element(By.XPATH, '//*[@id="multipleTenants"]/div/button')
            dropdown_menu.click()

            uk_organisations = chrome_driver.find_element(By.XPATH, '//*[@id="c1fe45a2-3d6b-43e7-a65e-c39443dc1448"]/button/span')
            uk_organisations.click()

            logger.debug('UK organisations selected')

            allow_access_button = chrome_driver.find_element(By.XPATH, '//*[@id="approveButton"]')
            allow_access_button.click()

            logger.debug('Now authorizing access request...')
            chrome_driver.implicitly_wait(30)

            logger.debug('Request successfully approved...now generating authorization code')
            
            url_with_authorization_code = chrome_driver.current_url
            
            start_no = url_with_authorization_code.find('code=')
            end_no = url_with_authorization_code.find('&scope')
            authorization_code = url_with_authorization_code[start_no:end_no][5:]


            self.authorization_code=authorization_code
        

            logger.debug('Authorization code successfully generated')
        
        except Exception as e:
            print(e)
            logger.error('Unable to process authorization request- see traceback log for more info')



        print('----------------------')
        print('AUTHORIZATION CODE SUCCESSFULLY EXTRACTED')
        print('----------------------')
        print(f'Authorization code: {self.authorization_code}')
        print(f'Code verifier: {self.code_verifier}')
        print(f'Code challenge: {self.code_challenge}')
        print(f'Client ID: {self.client_id}')
        print(f'Scope: {self.scope}')

        print(f'Redirect URI: {self.redirect_uri}')
        print(f'Response Type: {self.response_type}')
        print(f'Code challenge method: {self.code_challenge_method}')
        print(f'State: {self.state}')

        # chrome_driver.close()


        return authorization_code    



    def log_in_to_xero_dev_console():
        pass

    def select_uk_organisation(self):
        pass    



# Exchange authorization code for access + refresh tokens 

    def run_code_exchange_with_xero_server(self): 

        authorization_code = self.authorization_code
        exchange_url = self.exchange_url
        client_id = self.client_id
        redirect_uri = self.redirect_uri
        code_verifier = self.code_verifier

        print(f'-------------------------------- ')
        print('BEGIN CODE EXCHANGE WITH XERO SERVER')
        print(f'-------------------------------- ')
        print(f'Authorization code: {authorization_code} ')
        print(f'Exchange URL: {exchange_url} ')
        print(f'Client ID: {client_id} ')
        print(f'Redirect URI: {redirect_uri} ')
        print(f'Code verifier: {code_verifier} ')
        print(f'-------------------------------- ')
        print(f'-------------------------------- ')


        session = requests.Session()
        try:
            logger.debug(f'Now exchanging authorization code for access token with Xero server...')  
            logger.debug(f'POST request issued to: {exchange_url} ...')  

            exchange_response = session.post(exchange_url,
                                        headers={
                                            'Content-Type' : 'application/x-www-form-urlencoded'
                                        },
                                        data = {
                                            'grant_type': 'authorization_code',
                                            'client_id': client_id,
                                            'code': authorization_code,
                                            'redirect_uri': redirect_uri,
                                            'code_verifier': code_verifier
                                        })
            logger.debug('Exchange requests completed...')
            
            if exchange_response.status_code==200:
                logger.info(f' Successfully obtained access and refresh tokens ')  

                exchange_json_response = exchange_response.json()

                access_token = exchange_json_response['access_token']
                refresh_token = exchange_json_response['refresh_token']

                self.access_token = access_token
                self.refresh_token = refresh_token 

                logger.info(f'1/5 API requests completed...')  
            
            elif exchange_response.status_code==400:
                print(exchange_response)
                logger.error(f' Missing credentials entered into API request - check the tokens and codes from previous steps are not empty')   
            
            elif exchange_response.status_code==401:
                print(exchange_response)
                logger.error(f' You have entered invalid credentials - revise the credentials from previous steps ')   
                
            elif exchange_response.status_code==403:
                print(exchange_response)
                logger.error(f' You are not authorized to access the endpoint\'s resources')  

            elif exchange_response.status_code==429:
                print(exchange_response)
                logger.error(f' You have exceeded the API calls limit - give the endpoints a few minutes of rest before rerunning API requests') 

        
        except Exception as e: 
            print(e)


        return None






    
    def get_tenant_details(self):
        access_token = self.access_token
        connection_url = self.connection_url

        session = requests.Session()
        try:
            logger.debug(f'Now requesting for tenant ID from Xero API...')  
            logger.debug(f'GET request issued to: {connection_url} ...') 

            connection_response = session.get(connection_url,
                                                    headers={
                                                            'Authorization' : 'Bearer ' + access_token,
                                                            'Content-Type' : 'application/json'

                                                    })                   
            logger.debug('Tenant ID request completed...')    

                        
            if connection_response.status_code==200:
                logger.info(f' Successfully obtained tenant ID ')  

                connection_json_response = connection_response.json()

                tenant_id = ''

                for tenants in connection_json_response:
                    tenants_dict = tenants
                
                tenant_id = tenants_dict['tenantId']
                self.tenant_id = tenant_id
                
                
                logger.info(f'2/5 API requests completed...')  


            elif connection_response.status_code==400:
                print(connection_response)
                logger.error(f' Missing credentials entered into API request - check the tokens and codes from previous steps are not empty')   
            
            elif connection_response.status_code==401:
                print(connection_response)
                logger.error(f' You have entered invalid credentials - revise the credentials from previous steps ')   
                
            elif connection_response.status_code==403:
                print(connection_response)
                logger.error(f' You are not authorized to access the endpoint\'s resources')  

            elif connection_response.status_code==429:
                print(connection_response)
                logger.error(f' You have exceeded the API calls limit - give the endpoints a few minutes of rest before rerunning API requests') 

        
        except Exception as e: 
            print(e)
            

        return None
        

    def get_new_refresh_token(self) -> str:
        refresh_token = self.refresh_token
        refresh_token_url = self.refresh_token_url
        access_token = self.access_token
        client_id = self.client_id
        session = requests.Session()
        try:
            logger.debug(f'Now generating a new refresh token...')   
            logger.debug(f'GET request issued to: {refresh_token_url} ...') 
        
            refresh_token_response = session.post(refresh_token_url, 
                                        headers = {
                                            'Authorization': 'Basic ' + access_token,
                                            'Content-Type': 'application/x-www-form-urlencoded'                        
                                        },
                                        data={
                                            'grant_type' : 'refresh_token',
                                            'client_id': client_id,
                                            'refresh_token' : refresh_token
                                        })
            logger.debug(f'Refresh token requests completed...')  



            if refresh_token_response.status_code==200:
                logger.info(f' Successfully generated a new refresh token ')
                logger.info('3/5 API requests completed...')   

                refresh_token_json_response = refresh_token_response.json()

                new_refresh_token = refresh_token_response['refresh_token']
                new_access_token = refresh_token_response['access_token']

                self.new_refresh_token = new_refresh_token
                self.new_access_token = new_access_token 

                logger.info(f'3/5 API requests completed...')  


            elif refresh_token_response.status_code==400:
                print(refresh_token_response)
                logger.error(f' Missing credentials entered into API request - check the tokens and codes from previous steps are not empty')   
            
            elif refresh_token_response.status_code==401:
                print(refresh_token_response)
                logger.error(f' You have entered invalid credentials - revise the credentials from previous steps ')   
                
            elif refresh_token_response.status_code==403:
                print(refresh_token_response)
                logger.error(f' You are not authorized to access the endpoint\'s resources')  

            elif refresh_token_response.status_code==429:
                print(refresh_token_response)
                logger.error(f' You have exceeded the API calls limit - give the endpoints a few minutes of rest before rerunning API requests') 

        except Exception as e: 
            print(e)

  
        return None




    # Get the repeating invoices from the Xero endpoints
    def get_repeating_invoices(self):

        repeating_invoices_url = self.repeating_invoices_url
        access_token = self.access_token
        tenant_id = self.tenant_id

        session = requests.Session()

        try:
            logger.debug(f'Now requesting for all invoices from Xero Repeating Invoices endpoint...') 
            logger.debug(f'GET request issued to: {repeating_invoices_url} ...')  
        
            print(f'-------------------------------- ')
            print('Currently making API calls to extract repeating invoices from Xero endpoints using: ')
            print(f'Repeating Invoice URL: {repeating_invoices_url} ')
            print(f'Access token: {access_token} ')
            print(f'Tenant ID: {tenant_id} ')
            repeating_invoices_response = session.get(repeating_invoices_url,
                                    headers = {
                                            'Authorization': 'Bearer ' + access_token,
                                            'Xero-tenant-id': tenant_id,
                                            'Accept': 'application/json'
                                    })
        
            logger.debug('Repeating invoices requests completed...')

            if repeating_invoices_response.status_code==200:
                logger.info(f' Successfully obtained repeating invoices ')  

                repeating_invoices_json = repeating_invoices_response.json()   
                     
                self.repeating_invoices_json=repeating_invoices_json

                # for invoice in repeating_invoices_json:


                repeating_invoices = repeating_invoices_json['RepeatingInvoices']
                invoice_count = len(repeating_invoices)
                print(f'Invoice count: {invoice_count} ')  
                print('------------------------------')
                print('------------------------------') 

                
                logger.info(f'4/5 API requests completed...')  

                
            elif repeating_invoices_response.status_code==400:
                print(repeating_invoices_response)
                logger.error(f' Missing credentials entered into API request - check the tokens and codes from previous steps are not empty')   
                
            elif repeating_invoices_response.status_code==401:
                print(repeating_invoices_response)
                logger.error(f' You have entered invalid credentials - revise the credentials from previous steps ')   
                
            elif repeating_invoices_response.status_code==403:
                print(repeating_invoices_response)
                logger.error(f' You are not authorized to access the endpoint\'s resources')  

            elif repeating_invoices_response.status_code==429:
                print(repeating_invoices_response)
                logger.error(f' You have exceeded the API calls limit - give the endpoints a few minutes of rest before rerunning API requests') 

        except Exception as e:
            print(e)
            logger.error('Unable to process invoices request- see traceback log for more info')


    # Get the contacts from the Xero endpoints
    def get_contacts(self):

        contacts_url = self.contacts_url
        access_token = self.access_token
        tenant_id = self.tenant_id

        session = requests.Session()

        try:
            logger.debug(f'Now requesting for all contacts from Xero Contacts endpoint...') 
            logger.debug(f'GET request issued to: {contacts_url} ...')  
        
            print(f'-------------------------------- ')
            print('Currently making API calls to request for company contacts from Xero Contacts endpoint using: ')
            print(f'Contacts URL: {contacts_url} ')
            print(f'Access token: {access_token} ')
            print(f'Tenant ID: {tenant_id} ')
            contacts_response = session.get(contacts_url,
                                    headers = {
                                            'Authorization': 'Bearer ' + access_token,
                                            'Xero-tenant-id': tenant_id,
                                            'Accept': 'application/json'
                                    })
        
            logger.debug('Contacts requests completed...')

            if contacts_response.status_code==200:
                logger.info(f' Successfully obtained contacts from Contacts endpoint ') 

                contacts_json = contacts_response.json()

                contacts_str= json.dumps(contacts_json, indent=2)
                self.contacts_json=contacts_json

                contacts = contacts_json['Contacts']
                contacts_count = len(contacts) 
                print(f'Contacts count: {contacts_count} ')  
                print('------------------------------')
                print('------------------------------') 

                logger.info(f'5/5 API requests completed...')  

                
            elif contacts_response.status_code==400:
                print(contacts_response)
                logger.error(f' Missing credentials entered into API request - check the tokens and codes from previous steps are not empty')   
                
            elif contacts_response.status_code==401:
                print(contacts_response)
                logger.error(f' You have entered invalid credentials - revise the credentials from previous steps ')   
                
            elif contacts_response.status_code==403:
                print(contacts_response)
                logger.error(f' You are not authorized to access the endpoint\'s resources')  

            elif contacts_response.status_code==429:
                print(contacts_response)
                logger.error(f' You have exceeded the API calls limit - give the endpoints a few minutes of rest before rerunning API requests') 

        except Exception as e:
            print(e)
            logger.error('Unable to process request to Contacts endpoint - see traceback log for more info')



    # Save invoices to Blob container     
    
    def save_xero_repeating_invoices_to_blob(self):
        content_settings = ContentSettings(content_type='application/json')
        storage_account_url = os.getenv('STORAGE_ACCOUNT_URL')
        storage_account_key =  os.getenv('STORAGE_ACCOUNT_KEY')
        container_name=os.getenv('CONTAINER_NAME')
        blob_name=os.getenv('INVOICES_BLOB_NAME')
        repeating_invoices = self.repeating_invoices_json

        try:
            logger.debug('Now uploading repeating invoices into Blob container in JSON format...')
            
            blob_service_client_instance = BlobServiceClient(account_url=storage_account_url, credential=storage_account_key)
            blob_client_instance=blob_service_client_instance.get_blob_client(container_name, blob_name, snapshot=None)
            
            repeating_invoices_str = json.dumps(repeating_invoices, indent=4)
            blob_client_instance.upload_blob(repeating_invoices_str, overwrite=True, content_settings=content_settings)
            
            logger.info(f'Successfully uploaded invoices to Blob as {blob_name} ')
            logger.debug(f'--------------------------------------------------------')
            logger.info(f'SUCCESS: Invoice migration process from Xero PROD to Blob Storage successfully completed. Check to see invoices correspond to the format requested by the Chargebee team.')  
        
        except Exception as e:
            print(e)
            logger.error('Unable to upload invoice blob into Blob container - check trackback for more info')
            
    
    def save_xero_contacts_to_blob(self):
        content_settings = ContentSettings(content_type='application/json')
        storage_account_url = os.getenv('STORAGE_ACCOUNT_URL')
        storage_account_key =  os.getenv('STORAGE_ACCOUNT_KEY')
        container_name=os.getenv('CONTAINER_NAME')
        blob_name=os.getenv('CONTACTS_BLOB_NAME')
        contacts = self.contacts_json
    
        try:
            logger.debug('Now uploading contacts into Blob container in JSON format...')
            
            blob_service_client_instance = BlobServiceClient(account_url=storage_account_url, credential=storage_account_key)
            blob_client_instance=blob_service_client_instance.get_blob_client(container_name, blob_name, snapshot=None)
            
            contacts_json = json.dumps(contacts, indent=4)
            blob_client_instance.upload_blob(contacts_json, overwrite=True, content_settings=content_settings)
            
            logger.info(f'Successfully uploaded contacts to Blob as {blob_name} ')
            logger.debug(f'--------------------------------------------------------')
            logger.info(f'SUCCESS: Contacts migration process from Xero PROD to Blob Storage successfully completed. Check to see contacts correspond to the format requested by the Chargebee team.')  
        
        except Exception as e:
            print(e)
            logger.error('Unable to upload invoice blob into Blob container - check trackback for more info')




if __name__ == "__main__":

    xero_dev_bot  = XeroBot()


    # Generate code verifier and challenge

    xero_dev_bot.add_code_verifier()
    xero_dev_bot.add_code_challenge()


    # Add client_id and scope
    xero_dev_bot.add_client_id('54C3B67E24334AEA96850E4AA85C0F95')
    xero_dev_bot.add_scope("openid profile email accounting.transactions offline_access")


    xero_dev_bot.set_authorization_url()
    xero_dev_bot.run_authorization_url()
    

    xero_dev_bot.run_code_exchange_with_xero_server()
    xero_dev_bot.get_tenant_details()
    xero_dev_bot.get_new_refresh_token()
    xero_dev_bot.get_repeating_invoices()
    xero_dev_bot.get_contacts()
    xero_dev_bot.save_xero_repeating_invoices_to_blob()
    xero_dev_bot.save_xero_contacts_to_blob()


