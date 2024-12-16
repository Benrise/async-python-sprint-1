import os
import json
import threading
import logging

from typing import Dict, Any
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

from external.client import YandexWeatherAPI
from utils import get_url_by_city_name

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class DataFetchingTask:
    def __init__(self, cities: Dict[str, str], max_workers: int = 18):
        self.cities = cities
        self.results = {}
        self.max_workers = max_workers
        
    def _fetch_city_weather_data(self, city_name: str):
        try:
            logging.debug(f"Thread {threading.current_thread().name} started fetching weather data for {city_name}")
            url_with_data = get_url_by_city_name(city_name)
            response = YandexWeatherAPI.get_forecasting(url_with_data)
            logging.debug(f"Thread {threading.current_thread().name} finished fetching data for {city_name}")
            return {city_name: response}
        except Exception as e:
            logging.error(f"Error fetching data for {city_name} in thread {threading.current_thread().name}: {str(e)}")
            return {city_name: {"error": str(e)}}
    
    def save_results_to_json_file(
        self, 
        save_dir: str = 'data', 
        filename: str = 'data_fetching_results.json'
    ):
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
            logging.info(f"Directory {save_dir} created")

        file_path = os.path.join(save_dir, filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=4, sort_keys=True)
            logging.info(f"Results saved to {file_path}")
    
    def run(self) -> Dict[str, Any]:
        """ 
        Используем потоки, т.к. задача относится к IO-bound и GIL тут нам никак не помешает
        """
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = [
                pool.submit(self._fetch_city_weather_data, city_name) for city_name, _ in self.cities.items()
            ]
            
            for future in tqdm(futures, desc="Fetching weather data", unit="task"):
                try:
                    result = future.result()
                    self.results.update(result)
                except Exception as e:
                    city_name = future.args[0]
                    thread_name = threading.current_thread().name
                    
                    logging.error(f"Error fetching weather data for {city_name} in thread {thread_name}")
                    
                    self.results[city_name] = {"error": str(e)}

        return self.results

class DataCalculationTask:
    pass


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass
