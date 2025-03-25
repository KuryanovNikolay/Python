import json


class ResultDAO:
    def save_to_json(self, result, file_path, config) -> str:
        json_data = {
            "configFile": file_path,
            "configurationID": config.id,
            "configurationData": {
                "mode": config.mode,
                "path": config.path
            },
            "out": result
        }

        output_file = f"output_config_{config.id}.json"
        with open(output_file, 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, indent=4, ensure_ascii=False)

        return output_file
