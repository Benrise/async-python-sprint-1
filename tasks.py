import os
import json
import multiprocessing
import threading
import logging

from typing import Dict, Any
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from external.client import YandexWeatherAPI
from external.analyzer import DayInfo, deep_getitem, load_data
from utils import get_url_by_city_name

class ServiceClass:
    def save_results_to_json_file(
        self, 
        filename: str,
        save_dir: str = 'data', 
    ):
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
            logging.info(f"Directory {save_dir} created")

        file_path = os.path.join(save_dir, filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=4, sort_keys=True)
            logging.info(f"Results saved to {file_path}")

class DataFetchingTask(ServiceClass):
    def __init__(self, cities: Dict[str, str], max_workers: int = 18):
        self.cities = cities
        self.max_workers = max_workers
        self.results = {}
        
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
    
    def run(self) -> Dict[str, Any]:
        """ 
        Используем потоки, т.к. задача относится к IO-bound и GIL тут нам никак не помешает
        """
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = [
                pool.submit(self._fetch_city_weather_data, city_name) for city_name in self.cities.keys()
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

class DataCalculationTask(ServiceClass):
    def __init__(self, input_path: str = './data/data_fetching_results.json', day_hours_start: int = 9, day_hours_end: int = 19):
        self.input_path = input_path
        self.day_hours_start = day_hours_start
        self.day_hours_end = day_hours_end
        self.days_forecasts_accessor_key = 'forecasts'
        self.days_analyzed_forecasts_new_key = 'days'
        self.results = {}
    
    def _process_city_days(self, city_data, city_name):
        try:
            logging.debug(f"Process {multiprocessing.current_process().name} started calculating days info for {city_name}")
            days_data = deep_getitem(city_data, self.days_forecasts_accessor_key)
            if days_data is None:
                return []
            
            days_info = [DayInfo(raw_data=day_data).to_json() for day_data in days_data]
            logging.debug(f"Process {multiprocessing.current_process().name} finished calculating days info for {city_name}")
            return days_info
        except Exception as e:
            logging.error(f"Error calculating days info for {city_name} in process {multiprocessing.current_process().name}: {str(e)}")
            return [{city_name: {"error": str(e)}}]
        
    
    def run(self):
        """ 
        Используем процессы, т.к. задача относится к CPU-bound, ограничения GIL не коснутся
        """
        data: dict = load_data(self.input_path)
        days_info = []
        
        with ProcessPoolExecutor() as pool:            
            futures = {
                pool.submit(self._process_city_days, city_data, city_name): city_name
                for city_name, city_data in data.items()
            }
            
            for future in tqdm(futures, desc="Calculating days info", unit="task"):
                city_name = futures[future]
                try:
                    days_info = future.result()
                    data[city_name][self.days_analyzed_forecasts_new_key] = days_info
                
                except Exception as e:
                    process_name = multiprocessing.current_process().name
                    
                    logging.error(f"Error calculating days info for {city_name} in process {process_name}")
                    
                    self.results[city_name] = {"error": str(e)}
            
        self.results.update(data)
        return self.results

class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass
