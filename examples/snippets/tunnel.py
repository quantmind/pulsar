from pulsar.apps import http
import requests     # noqa

if __name__ == '__main__':
    session = http.HttpClient()
    # session = requests.session()
    #
    url = 'https://api.github.com/'
    verify = True
    #
    url = 'https://127.0.0.1:8070'
    verify = False
    #
    proxies = {'https': 'http://127.0.0.1:8060'}
    response = session.get(url, proxies=proxies, verify=verify)
    print(response)
    # proxies = {'https': 'http://127.0.0.1:8080'}
    # response = session.get(url, proxies=proxies, verify=verify)
    # print(response)
    proxies = {'https': 'http://127.0.0.1:8060'}
    response = session.get(url, proxies=proxies, verify=verify)
    print(response)
