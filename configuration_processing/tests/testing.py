import json
from pathlib import Path

import pytest

from presentation.CliHandler import CliHandler

PROJECT_ROOT = Path(r"/configuration_processing")
TEST_CONFIG_PATH = PROJECT_ROOT / "tests" / "test.txt"
ANSWERS_DIR = PROJECT_ROOT / "tests" / "answers"

EXPECTED_RESULTS = {
    1: ANSWERS_DIR / "output_config_1.json",
    2: ANSWERS_DIR / "output_config_2.json",
    3: ANSWERS_DIR / "output_config_3.json",
    4: ANSWERS_DIR / "output_config_4.json",
}


@pytest.fixture(scope="module")
def temp_dir():
    temp_path = PROJECT_ROOT / "temp_results"
    temp_path.mkdir(exist_ok=True)
    yield temp_path
    import shutil
    shutil.rmtree(temp_path)


def compare_json_files(actual_path, expected_path):
    with open(actual_path) as f_act, open(expected_path) as f_exp:
        actual = json.load(f_act)
        expected = json.load(f_exp)

        if actual != expected:
            diff = []
            for key in set(actual.keys()) | set(expected.keys()):
                if key not in actual:
                    diff.append(f"В результате отсутствует ключ: {key}")
                elif key not in expected:
                    diff.append(f"В результате лишний ключ: {key}")
                elif actual[key] != expected[key]:
                    diff.append(f"Различие в '{key}': {expected[key]} != {actual[key]}")
            return False, "\n".join(diff)
        return True, ""


@pytest.mark.parametrize("config_num", [1, 2, 3, 4])
def test_config_processing(config_num, temp_dir):
    if not TEST_CONFIG_PATH.exists():
        pytest.fail(f"Тестовый конфиг не найден: {TEST_CONFIG_PATH}")

    expected_path = EXPECTED_RESULTS[config_num]
    if not expected_path.exists():
        pytest.skip(f"Ожидаемый результат не найден: {expected_path}")

    try:
        handler = CliHandler(str(TEST_CONFIG_PATH), config_num)
        actual_output_path = handler.handle()

        actual_path = Path(actual_output_path)
        if not actual_path.exists():
            pytest.fail(f"Файл результата не создан: {actual_path}")

        is_match, diff = compare_json_files(actual_path, expected_path)
        if not is_match:
            pytest.fail(f"Результат не соответствует ожидаемому:\n{diff}")

        result_path = temp_dir / f"result_config_{config_num}.json"
        actual_path.rename(result_path)

    except Exception as e:
        pytest.fail(f"Ошибка при обработке конфигурации {config_num}:\n{str(e)}")
