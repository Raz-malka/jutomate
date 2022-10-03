import os
import json



class Files:

    @classmethod
    def get_new_path(cls, *paths: str or int) -> str:
        str_path = [str(path) for path in paths]
        new_path = os.path.join(*str_path)
        return new_path


    @classmethod
    def make_file(cls, data, *paths: str or int, file_name: str, file_type: str='.json'):
        # if file exisits it will update file
        # make folders
        new_path = cls.get_new_path(*paths)
        full_path = os.path.join(new_path, file_name)
        if os.path.exists(new_path):
            if os.path.exists(full_path):
                # get data on file
                with open(full_path + file_type, 'r', encoding='utf-8') as file:
                    file_data = json.load(file) # if file empty!!
                    file_data.update(data)
                    data = file_data
        else:
            os.makedirs(new_path)

        # make new/updated data file
        with open(full_path + file_type, 'w', encoding='utf-8') as file:
            if file_type == '.json':
                json.dump(data, file, indent=4, ensure_ascii=False)
            elif file_type == '.csv':
                pass # not relavent yet
            #elif file_type == '.jpg':


    @classmethod
    # incomplete - not relavent yet
    def json_to_csv(cls, data: list):
        pass     
            