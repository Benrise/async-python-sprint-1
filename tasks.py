import csv
import os
import json
import multiprocessing
import threading
import logging

from datetime import datetime
from multiprocessing import Manager
from typing import Dict, Any, List
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

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
        max_workers: int = 18
    ):
        self.input_path = input_path
        self.max_workers = max_workers
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
    
    def _get_temperature_values(self, city_data: Dict) -> List[float]:
        return [round(day['temp_avg']) if day.get('temp_avg') is not None else 0
                for day in city_data.get('days', [])]
    
    def _get_rain_hours_values(self, city_data: Dict) -> List[float]:
        return [day['relevant_cond_hours'] if day.get('relevant_cond_hours') is not None else 0
                for day in city_data.get('days', [])]
    
    def _calculate_avg(self, values: List[float]) -> float:
        return sum(values) / len(values) if values else 0
    
    def _process_city_stats(self, city_name: str, city_data: Dict) -> Dict:
        temperature_values = self._get_temperature_values(city_data)
        rain_hours_values = self._get_rain_hours_values(city_data)
        
        avg_temp = self._calculate_avg(temperature_values)
        avg_rain_hours = self._calculate_avg(rain_hours_values)
        
        return {
            'avg_temp': avg_temp,
            'avg_rain_hours': avg_rain_hours,
            'temperature_values': temperature_values,
            'rain_hours_values': rain_hours_values
        }


    def run(self) -> Dict[str, Any]:
        data: dict = self.load_json_data(self.input_path)

        processed_data = {}
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_city = {
                executor.submit(self._process_city_stats, city_name, city_forecast): city_name
                for city_name, city_forecast in data.items()
            }

            for future in as_completed(future_to_city):
                city_name = future_to_city[future]
                try:
                    city_stats = future.result()
                    processed_data[city_name] = city_stats
                except Exception as e:
                    print(f"Error processing {city_name}: {e}")

        days_period = self._get_days_period(data)

        return {
            'days_period': days_period,
            'data': processed_data,
        }

class DataAnalyzingTask:
    def __init__(
        self,
        output_path: str = './data/final_results',
        output_format: str = 'csv',
        max_workers: int = 18
    ):
        self.output_path = output_path + '.' + output_format
        self.output_format = output_format
        self.max_workers = max_workers
        
    def _calculate_ratings(self, data: Dict[str, Dict]) -> Dict[str, int]:
        sorted_cities = sorted(data.items(), key=lambda item: (item[1]['avg_temp'], item[1]['avg_rain_hours']), reverse=True)
        
        ratings = {city_name: index + 1 for index, (city_name, _) in enumerate(sorted_cities)}
        
        return ratings

    def _save_to_csv(self, data: Dict[str, Dict], output_path: str, days_period: List[str], ratings: Dict[str, int]):
        """ 
        Используем потоки, т.к. задача относится к IO-bound, записываем данные в csv файл
        """
        headers = ['Город/день', '', *days_period, 'Среднее', 'Рейтинг']

        with open(output_path, mode='w', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._process_city_csv_data, city_name, city_stats, ratings[city_name]): city_name
                    for city_name, city_stats in data.items()
                }

                for future in as_completed(futures):
                    city_name = futures[future]
                    try:
                        row_temp, row_rain = future.result()
                        writer.writerow(row_temp)
                        writer.writerow(row_rain)
                    except Exception as e:
                        logging.error(f"Error while processing {city_name} to .csv file: {e}")
    
    def _process_city_csv_data(self, city_name: str, city_stats: Dict, rating: int) -> List:
        row_temp = [city_name, 'Температура, среднее', *city_stats['temperature_values'], city_stats['avg_temp'], rating]
        row_rain = ['', 'Без осадков, часов', *city_stats['rain_hours_values'], city_stats['avg_rain_hours'], ''] 
        
        return row_temp, row_rain
    
    def run(self, aggregation_results: Dict[str, Any]):
        data = aggregation_results['data']
        days_period = aggregation_results['days_period']

        ratings = self._calculate_ratings(data)

        if self.output_format == 'csv':
            self._save_to_csv(data, self.output_path, days_period, ratings)
        else:
            raise ValueError("Поддерживается только формат CSV")