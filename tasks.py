import csv
import os
import json
import multiprocessing
import threading
import logging

from datetime import datetime
from multiprocessing import Manager, Pool
from typing import Dict, Any, List
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from external.client import YandexWeatherAPI
from external.analyzer import DayInfo, deep_getitem
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

    def load_json_data(
        self,
        filename: str,
        load_dir: str = 'data'
    ) -> Dict:
        with open(f"{load_dir}/{filename}", 'r', encoding='utf-8') as file:
            return json.load(file)

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
                    logging.error(f"Error fetching data for {city_name}: {str(e)}")

        return self.results

class DataCalculationTask(ServiceClass):
    def __init__(self, input_path: str = 'fetching_results.json', day_hours_start: int = 9, day_hours_end: int = 19, max_workers: int = 18):
        self.input_path = input_path
        self.day_hours_start = day_hours_start
        self.day_hours_end = day_hours_end
        self.days_forecasts_accessor_key = 'forecasts'
        self.days_analyzed_forecasts_new_key = 'days'
        self.max_workers = max_workers
        self.results = {}
    
    def _process_city_days(self, city_data, city_name, queue):
        try:
            logging.debug(f"Process {multiprocessing.current_process().name} started calculating days info for {city_name}")
            days_data = deep_getitem(city_data, self.days_forecasts_accessor_key)
            if days_data is None:
                queue.put((city_name, {}))
                return {}
            
            days_info = [DayInfo(raw_data=day_data).to_json() for day_data in days_data]
            logging.debug(f"Process {multiprocessing.current_process().name} finished calculating days info for {city_name}")

            queue.put((city_name, {self.days_analyzed_forecasts_new_key: days_info}))
            return days_info
        except Exception as e:
            logging.error(f"Error calculating days info for {city_name} in process {multiprocessing.current_process().name}: {str(e)}")
            queue.put((city_name, {"error": str(e)}))
            return [{city_name: {"error": str(e)}}]
        
    
    def run(self):
        """ 
        Используем процессы, т.к. задача относится к CPU-bound, ограничения GIL не коснутся
        Используем очередь класса Manager для контрольного доступа к общему словарю results
        """
        data: dict = self.load_json_data(self.input_path)
        manager = Manager()
        queue = manager.Queue()
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as pool:            
            futures = {
                pool.submit(self._process_city_days, city_data, city_name, queue): city_name
                for city_name, city_data in data.items()
            }
            
            for future in tqdm(futures, desc="Calculating days info", unit="task"):
                city_name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error calculating days info for {city_name}: {str(e)}")
            
            while not queue.empty():
                city_name, city_result = queue.get()
                self.results[city_name] = city_result

        return self.results

class DataAggregationTask(ServiceClass):
    def __init__(
        self, 
        input_path: str = 'calculating_results.json',
        output_path: str = './data/aggregation_results',
        output_format: str = 'csv',
        max_workers: int = 18
    ):
        self.input_path = input_path
        self.output_path = output_path + '.' + output_format
        self.output_format = output_format
        max_workers = max_workers
        self.results = []
        
        
    def _get_days_period(self, data: Dict[str, Dict]) -> List[str]:
        days_period = set()
        
        for city_data in data.values():
            if 'days' in city_data and city_data['days']:
                for day in city_data['days']:
                    if day.get('date'):
                        date_str: str = day['date']
                        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                        month_day = date_obj.strftime('%m-%d')
                        days_period.add(month_day)
                        
        return days_period

    def _process_city_csv_data(self, city_name: str, city_data: Dict, days: List[str]) -> List:
        temperature_values = []
        rain_hours_values = []

        for day in city_data.get('days', []):
            if day.get('temp_avg') is not None:
                temperature_values.append(round(day['temp_avg']))
            else:
                temperature_values.append(0)

            if day.get('relevant_cond_hours') is not None:
                rain_hours_values.append(day['relevant_cond_hours'])
            else:
                rain_hours_values.append(0)

        avg_temp = sum(temperature_values) / len(temperature_values) if temperature_values else 0
        avg_rain_hours = sum(rain_hours_values) / len(rain_hours_values) if rain_hours_values else 0

        row_temp = [city_name, 'Температура, среднее', *temperature_values, avg_temp, '']
        row_rain = ['', 'Без осадков, часов', *rain_hours_values, avg_rain_hours, ''] 
        
        return row_temp, row_rain
    
    def save_to_csv(self, data: Dict[str, Dict], output_path: str):
        days = self._get_days_period(data)
        headers = ['Город/день', '', *days, 'Среднее']
        
        with open(output_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)

            with Pool() as pool:
                results = pool.starmap(
                    self._process_city_csv_data, 
                    [(city_name, city_data, days) for city_name, city_data in data.items() if 'days' in city_data]
                )

                for row_temp, row_rain in results:
                    writer.writerow(row_temp)
                    writer.writerow(row_rain)
            
    def run(self):
        data: dict = self.load_json_data(self.input_path)
        
        if (self.output_format == 'csv'):
            self.save_to_csv(data, output_path=self.output_path)
            
        

class DataAnalyzingTask:
    pass
