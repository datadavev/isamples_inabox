from typing import Optional

import requests
import click
import json

from requests import Response

token_option = click.option(
    "-t",
    "--identity_token",
    type=str,
    default=None,
    help="The identifier token",
)
url_option = click.option(
    "-u",
    "--url",
    type=str,
    default="https://central.isample.xyz/isamples_central/manage/mint_draft_datacite_identifiers",
    help="The url to the mint identifiers endpoint",
)
identifiers_option = click.option(
    "-n",
    "--num_identifiers",
    type=str,
    default="1",
    help="The number of identifiers to create",
)
session = requests.session()


@click.group()
def main():
    pass


def _post_to_mint_method(post_data: dict, identity_token: str, url: str) -> Response:
    post_bytes = json.dumps(post_data).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {identity_token}",
    }
    print(f"Attempting to post to {url} to mint identifiers…")
    response = session.post(url, post_bytes, headers=headers)
    if response.status_code == 200:
        print(f"Success response: {response}")
    else:
        print(f"Error response: {response}")
    return response


@main.command("mint_datacite_identifiers")
@token_option
@url_option
@identifiers_option
@click.option(
    "-f",
    "--file",
    type=str,
    help="The path to the metadata JSON file, in the datacite format per https://support.datacite.org/docs/api-create-dois",
)
def mint_datacite_identifiers(identity_token: str, url: str, num_identifiers: int, file: str):
    with open(file) as json_file:
        data = json.load(json_file)
        post_data = {"num_drafts": num_identifiers, "datacite_metadata": data}
        _post_to_mint_method(post_data, identity_token, url)


@main.command("mint_noidy_identifiers")
@token_option
@url_option
@identifiers_option
@click.option(
    "-f",
    "--file",
    type=str,
    default=None,
    help="The path to the output file",
)
@click.option(
    "-s",
    "--shoulder",
    type=str,
    help="The shoulder to use for identifier generation",
)
def mint_noidy_identifiers(identity_token: str, url: str, num_identifiers: int, file: Optional[str], shoulder: str):
    post_data = {"num_identifiers": num_identifiers, "shoulder": shoulder}
    if file is not None:
        post_data["return_filename"] = file
    response = _post_to_mint_method(post_data, identity_token, url)
    if file is not None:
        with open(file, "w") as f:
            f.write(response.text)
            print(f"Successfully saved csv file with {num_identifiers} newly minted identifiers to {file}")
    else:
        print(f"Successfully minted {num_identifiers} identifiers.")
        print(response.json())


@main.command("create_namespace")
@token_option
@url_option
@click.option(
    "-s",
    "--shoulder",
    type=str,
    help="The shoulder to use for identifier generation",
)
@click.option(
    "-i",
    "--orcid_ids",
    type=str,
    default=None,
    help="The orcid ids permitted to create identifiers in the specified namespace, comma delimited",
)
def create_namespace(identity_token: str, url: str, shoulder: str, orcid_ids: str):
    post_data = {"orcid_ids": orcid_ids.split(","), "shoulder": shoulder}
    response = _post_to_mint_method(post_data, identity_token, url)
    print("Successfully created namespace.")
    print(response.json())


if __name__ == "__main__":
    main()
