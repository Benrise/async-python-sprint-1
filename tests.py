import unittest
from unittest.mock import mock_open, patch


from tasks import DataAnalyzingTask


class TestDataAnalyzingTask(unittest.TestCase):
    def setUp(self):
        self.data_analyzing_task = DataAnalyzingTask()
    
    def test_calculate_ratings(self):
        data = {
            'CityA': {'avg_temp': 25, 'avg_rain_hours': 50},
            'CityB': {'avg_temp': 30, 'avg_rain_hours': 40},
            'CityC': {'avg_temp': 30, 'avg_rain_hours': 60},
            'CityD': {'avg_temp': 20, 'avg_rain_hours': 30},
        }

        expected_ratings = {
            'CityB': 2,
            'CityC': 1,
            'CityA': 3,
            'CityD': 4,
        }

        ratings = self.data_analyzing_task._calculate_ratings(data)
        
        self.assertEqual(ratings, expected_ratings)
        
    def test_process_city_csv_data(self):
        city_name = 'CityA'
        city_stats = {
            'temperature_values': [20, 22, 24],
            'rain_hours_values': [5, 3, 2],
            'avg_temp': 22,
            'avg_rain_hours': 3.33
        }
        rating = 1

        expected_row_temp = ['CityA', 'Температура, среднее', 20, 22, 24, 22, 1]
        expected_row_rain = ['', 'Без осадков, часов', 5, 3, 2, 3.33, '']

        row_temp, row_rain = self.data_analyzing_task._process_city_csv_data(city_name, city_stats, rating)

        self.assertEqual(row_temp, expected_row_temp)
        self.assertEqual(row_rain, expected_row_rain)
        
    @patch('builtins.open', new_callable=mock_open)
    @patch('csv.writer')
    def test_save_to_csv(self, mock_csv_writer, mock_open):
        mock_writer = mock_csv_writer.return_value
        data = {
            'CityA': {
                'temperature_values': [20, 22, 24],
                'rain_hours_values': [5, 3, 2],
                'avg_temp': 22,
                'avg_rain_hours': 3.33
            }
        }
        days_period = ['2024-01-01', '2024-01-02', '2024-01-03']
        ratings = {'CityA': 1}

        self.data_analyzing_task._save_to_csv(data, 'test_output.csv', days_period, ratings)

        self.assertEqual(mock_writer.writerow.call_count, 3)
        
        mock_writer.writerow.assert_any_call(['CityA', 'Температура, среднее', 20, 22, 24, 22, 1])
        mock_writer.writerow.assert_any_call(['', 'Без осадков, часов', 5, 3, 2, 3.33, ''])

    @patch('tasks.DataAnalyzingTask._calculate_ratings')
    @patch('tasks.DataAnalyzingTask._save_to_csv')
    def test_run(self, mock_save_to_csv, mock_calculate_ratings):
        mock_calculate_ratings.return_value = {'CityA': 1, 'CityB': 2}
        aggregation_results = {
            'data': {'CityA': {'avg_temp': 22, 'avg_rain_hours': 5}, 'CityB': {'avg_temp': 25, 'avg_rain_hours': 4}},
            'days_period': ['2024-01-01', '2024-01-02', '2024-01-03']
        }

        self.data_analyzing_task.run(aggregation_results)

        mock_calculate_ratings.assert_called_once_with(aggregation_results['data'])

        mock_save_to_csv.assert_called_once_with(
            aggregation_results['data'], 
            self.data_analyzing_task.output_path, 
            aggregation_results['days_period'], 
            {'CityA': 1, 'CityB': 2}
        )
        
    def test_run_invalid_format(self):
        self.data_analyzing_task.output_format = 'json'
        with self.assertRaises(ValueError):
            self.data_analyzing_task.run({
                'data': {'CityA': {'avg_temp': 22, 'avg_rain_hours': 5}},
                'days_period': ['2024-01-01']
            })

    @patch('builtins.open', new_callable=mock_open)
    @patch('csv.writer')
    def test_save_to_csv_multiple_cities(self, mock_csv_writer, mock_open):
        mock_writer = mock_csv_writer.return_value
        data = {
            'CityA': {'temperature_values': [20, 22, 24], 'rain_hours_values': [5, 3, 2], 'avg_temp': 22, 'avg_rain_hours': 3.33},
            'CityB': {'temperature_values': [18, 19, 20], 'rain_hours_values': [10, 9, 8], 'avg_temp': 19, 'avg_rain_hours': 9}
        }
        days_period = ['2024-01-01', '2024-01-02', '2024-01-03']
        ratings = {'CityA': 1, 'CityB': 2}

        self.data_analyzing_task._save_to_csv(data, 'test_output.csv', days_period, ratings)

        mock_writer.writerow.assert_any_call(['CityA', 'Температура, среднее', 20, 22, 24, 22, 1])
        mock_writer.writerow.assert_any_call(['', 'Без осадков, часов', 5, 3, 2, 3.33, ''])
        mock_writer.writerow.assert_any_call(['CityB', 'Температура, среднее', 18, 19, 20, 19, 2])
        mock_writer.writerow.assert_any_call(['', 'Без осадков, часов', 10, 9, 8, 9, ''])