import typer
from typing_extensions import Annotated
from app.services.download_service import download_service
from app.utils import Validator
from typing import List

app = typer.Typer()

class DowloadFilterValidator(Validator):
    pass

@app.command("list", help="List out all the configs.")
def get_downloads(
        filter: Annotated[str, typer.Option(help="Filter by some value. Format: filter_key=filter_value.")] = None
    ):
    if filter:
        validator = DowloadFilterValidator(valid_keys=["client_name"])

        try:
            data = validator.validate(filters=[filter])
            client_name = data['client_name']
        except Exception as e:
            print(f"filter error: {e}")
            return 
        
        print(download_service._list_all_downloads_by_client_name(client_name))
        return 

    print(download_service._list_all_downloads())

@app.command("add", help="Submit new config to download.")
def submit_download(
        file_path: Annotated[str, typer.Argument(help="File path of config to be uploaded.")],
        license: Annotated[List[str], typer.Option("--license", "-l", help="License ID.")] = [],
    ):
    if len(license) == 0:
        print("No licenses mentioned. Please specify licenese Id.")
        return
    
    print(download_service._add_new_download(file_path, license))

@app.command("get", help="Get a particular config.")
def get_download_by_config(
        config_name: Annotated[str, typer.Argument(help="Config file name.")]
    ):
    print(download_service._get_download_by_config(config_name))

@app.command("remove", help="Remove existing config.")
def remove_download(
        config_name: Annotated[str, typer.Argument(help="Config file name.")]
    ):
    print(download_service._remove_download(config_name))