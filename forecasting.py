import logging

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES
from external.analyzer import parse_args


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    data_fetching_task = DataFetchingTask(CITIES)
    data_fetching_task.run()
    data_fetching_task.save_results_to_json_file('fetching_results.json')
    
    data_calculation_task = DataCalculationTask()
    data_calculation_task.run()
    data_calculation_task.save_results_to_json_file('calculating_results.json')
    
    data_aggregation_task = DataAggregationTask()
    data = data_aggregation_task.run()
    
    data_analyzing_task = DataAnalyzingTask()
    data_analyzing_task.run(data)


if __name__ == "__main__":
    args = parse_args()
    verbose_mode = args.verbose
    
    logging.basicConfig(level=logging.DEBUG if verbose_mode else logging.WARNING)
    forecast_weather()
