import requests
import json




def main():
    print("todo bien")

def facturas():
    url = "https://api.alegra.com/api/v1/invoices?metadata=true"    


    headers = {
        "accept": "application/json",
        "authorization": "Basic bmFub3Ryb25pY3NhbHNvbmRlbGF0ZWNub2xvZ2lhQGdtYWlsLmNvbTphMmM4OTA3YjE1M2VmYTc0ODE5ZA=="
    }

    response = requests.get(url, headers=headers)
    #luego cargamos para saber cual fue la ultima facutura que tenemos guardada
    with open('facturas.json', 'r') as f:
        datos = json.load(f)
        inicio = datos[-1]['id']



    for i in range(int(inicio), int(response.json()['metadata']['total']),30):
        url2 = f"https://api.alegra.com/api/v1/invoices?start={i}&order_direction=ASC&order_field=id"
        # Obtener la respuesta
        response2 = requests.get(url2, headers=headers)

        data = response2.json()

        # Guardar en un archivo con formato
        with open('facturas.json', 'w') as f:
            json.dump(data, f, indent=4)

  


if __name__  == "__main__":
    main()