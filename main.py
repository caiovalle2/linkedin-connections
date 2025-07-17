import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dotenv import load_dotenv
import time
import pandas as pd  # Usando pandas para salvar
from typing import List, Dict

class LinkedInScraperRequests:
    BASE_URL = "https://www.linkedin.com"
    LOGIN_URL = "https://www.linkedin.com/login"
    LOGIN_SUBMIT_URL = "https://www.linkedin.com/checkpoint/lg/login-submit"
    CONNECTIONS_URL = "https://www.linkedin.com/mynetwork/invite-connect/connections/"

    def __init__(self, email, password):
        self.session = requests.Session()
        self.email = email
        self.password = password
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        })

    def _get_login_form_data(self, html):
        soup = BeautifulSoup(html, "html.parser")
        form = soup.find("form", {"class": "login__form"})
        if not form:
            raise Exception("Formulário de login não encontrado")

        data = {}
        for input_tag in form.find_all("input"):
            name = input_tag.get("name")
            value = input_tag.get("value", "")
            if name:
                data[name] = value
        return data

    def login(self):
        resp = self.session.get(self.LOGIN_URL)
        resp.raise_for_status()

        form_data = self._get_login_form_data(resp.text)

        form_data["session_key"] = self.email
        form_data["session_password"] = self.password

        post_resp = self.session.post(self.LOGIN_SUBMIT_URL, data=form_data)
        post_resp.raise_for_status()

        if "feed" not in post_resp.url and "checkpoint" not in post_resp.url:
            raise Exception("Login falhou, verifique as credenciais ou se há CAPTCHA.")

        print("Login realizado com sucesso.")
    
    def get_connections_html(self):
        resp = self.session.get(self.CONNECTIONS_URL)
        time.sleep(3)
        resp.raise_for_status()
        return resp.text

    def parse_connections(self, html) -> List[Dict]:
        soup = BeautifulSoup(html, "html.parser")
        connection_blocks = soup.select('div[data-view-name="connections-list"] > div')
        connections = []
        for block in connection_blocks:
            try:
                name_tag = block.find('a', {'data-view-name': 'connections-profile'})
                name = name_tag.get_text(strip=True) if name_tag else "N/A"
                profile_url = name_tag['href'] if name_tag and name_tag.has_attr('href') else "N/A"
                occupation_p = block.find_all('p')
                occupation = occupation_p[1].get_text(strip=True) if len(occupation_p) > 1 else "N/A"
                date_text = next((p.get_text(strip=True) for p in occupation_p if "Conexão feita em" in p.text), "N/A")
                img_tag = block.find('img', alt=lambda x: x and "Foto do perfil de" in x)
                profile_img = img_tag['src'] if img_tag and img_tag.has_attr('src') else "N/A"
                connections.append({
                    'name': name,
                    'occupation': occupation,
                    'profile_url': profile_url,
                    'connected_on': date_text,
                    'profile_image': profile_img
                })
            except Exception as e:
                print(f"[ERRO] Falha ao processar uma conexão: {e}")
                continue
        return connections


def run_etl():
    load_dotenv()
    email = os.getenv("LINKEDIN_USER")
    password = os.getenv("LINKEDIN_PASSWORD")

    scraper = LinkedInScraperRequests(email, password)
    scraper.login()
    html = scraper.get_connections_html()
    connections = scraper.parse_connections(html)

    # Salvar em CSV com pandas
    df = pd.DataFrame(connections)
    df.to_csv("connections.csv", index=False, encoding="utf-8")
    print("Conexões salvas no arquivo connections.csv")

    # Printar na tela
    print(df)

if __name__ == "__main__":
    run_etl()
