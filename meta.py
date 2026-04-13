# import requests
# import json

# HEADERS = {"Referer": "https://www.vietnamworks.com/"}

# endpoints = [
#     "highest-degrees",
#     "job-levels",
#     "company-sizes",
# ]

# for endpoint in endpoints:
#     url = f"https://ms.vietnamworks.com/meta/v1.0/{endpoint}"
#     r   = requests.get(url, headers=HEADERS)
#     print(f"\n{'='*60}")
#     print(f" {endpoint}")
#     print(f"{'='*60}")
#     print(json.dumps(r.json(), ensure_ascii=False, indent=2))

# import requests

# r = requests.get(
#     "https://ms.vietnamworks.com/job-search/v1.0/job/2031253",
#     headers={
#         "Referer": "https://www.vietnamworks.com/",
#         "Accept" : "application/json",
#     }
# )
# print(r.status_code)
# print(r.text[:500])
# # Thử các endpoint khác nhau
# endpoints = [
#     "https://ms.vietnamworks.com/job-search/v1.0/job/2031253",
#     "https://ms.vietnamworks.com/job-search/v2.0/job/2031253",
#     "https://ms.vietnamworks.com/job/v1.0/2031253",
# ]




# for url in endpoints:
#     r = requests.get(url, headers={"Referer": "https://www.vietnamworks.com/"})
#     print(f"{r.status_code} | {url}")
#     if r.status_code == 200:
#         print(r.text[:200])


import requests, json

r = requests.post(
    "https://ms.vietnamworks.com/job-search/v1.0/search",
    headers={
        "Content-Type": "application/json",
        "Accept"      : "application/json",
        "Referer"     : "https://www.vietnamworks.com/",
    },
    json={
        "jobFunction": 5,
        "page"       : 0,
        "hitsPerPage": 1,
    }
)

job = r.json()["data"][0]
print("jobDescription:", job.get("jobDescription", "")[:300])
print("jobRequirement:", job.get("jobRequirement", "")[:300])
print("jobFunction:", job.get("jobFunction"))
print("jobFunctionsV3:", job.get("jobFunctionsV3"))