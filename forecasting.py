# import logging
# import threading
# import subprocess
# import multiprocessing


from external.client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, get_url_by_city_name


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    data_fetching_task = DataFetchingTask(CITIES)
    data_fetching_task.run()
    data_fetching_task.save_results_to_json_file('data_fetching_results.json')
    
    data_calculation_task = DataCalculationTask()
    data_calculation_task.run()
    data_calculation_task.save_results_to_json_file('data_calculating_results.json')


if __name__ == "__main__":
    forecast_weather()
