
import csv
import io
import asyncio
from subsets_utils import get, save_raw_json


def fetch_all_groups() -> list:
    """Fetch all available groups from Bank of Canada API"""
    response = get("https://www.bankofcanada.ca/valet/lists/groups/csv", timeout=30.0)
    response.raise_for_status()
    
    lines = response.text.split('\n')
    
    data_start = -1
    for i, line in enumerate(lines):
        if line.strip() == 'GROUPS':
            data_start = i + 1
            break
    
    if data_start == -1:
        raise ValueError("Could not find GROUPS section in response")
    
    csv_content = '\n'.join(lines[data_start:])
    reader = csv.DictReader(io.StringIO(csv_content))
    
    return list(reader)


def get_group_details_sync(group_name: str) -> dict:
    url = f"https://www.bankofcanada.ca/valet/groups/{group_name}/json"
    response = get(url, timeout=30.0)
    response.raise_for_status()
    return response.json()


async def get_group_details_async(group_name: str, semaphore) -> dict:
    async with semaphore:
        return await asyncio.to_thread(get_group_details_sync, group_name)


def run():
    groups_list = fetch_all_groups()
    print(f"Fetched {len(groups_list)} groups")
    
    async def fetch_all_details():
        semaphore = asyncio.Semaphore(10)
        tasks = [get_group_details_async(g['name'], semaphore) for g in groups_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_data = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Error fetching group {groups_list[i]['name']}: {result}")
            else:
                all_data.append(result)
        return all_data
    
    details = asyncio.run(fetch_all_details())
    print(f"Fetched details for {len(details)} groups")
    save_raw_json(details, "groups")
