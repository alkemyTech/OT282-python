from datetime import datetime, date
import pandas as pd

URL_LOCATION = "https://drive.google.com/file/d/1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ/view"
URL_GOOGLE_EXPORT = "https://drive.google.com/uc?export=download&id="

# This function converts given date to age
def age(born):
    """Transforms a given date of birth into age"""
    born = datetime.strptime(born, "%Y-%m-%d").date()
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


def process_files(input_path, output_path):
    """Process both universities at the same time in order to be included in a PythonOperator"""

    process_comahue(
        comahue_input_path=input_path + "comahue_university.csv",
        comahue_output_path=output_path + "comahue.txt",
    )
    process_delsalvador(
        delsalvador_input_path=input_path + "delsalvador_university.csv",
        delsalvador_output_path=output_path + "delsalvador.txt",
    )


def process_comahue(comahue_input_path, comahue_output_path):
    """Process the comahue file

    Parameters
    ----------
    comahue_input_path: str
        str with the location of the files that are going to be processed
        Example:
            "dags/files/comahue_university.csv"
    comahue_output_path: str
        str with the location of the txt that will be generated after processing the input files
        Example:
            "dags/files/output/comahue.txt"

    Returns
    -------
    None
    """

    url = URL_LOCATION
    path = URL_GOOGLE_EXPORT + url.split("/")[-2]
    location = pd.read_csv(path)
    comahue = pd.read_csv(comahue_input_path)

    # COMAHUE PROCESSING

    comahue["full_name"] = (
        comahue["full_name"]
        .str.lstrip("DR.")
        .str.lstrip("MISS")
        .str.lstrip("MR.")
        .str.lstrip("MRS.")
        .str.rstrip("MD")
        .str.rstrip("DDS")
        .str.rstrip("JR.")
        .str.rstrip("PHD")
        .str.rstrip("II")
        .str.rstrip("DVM")
        .str.strip()
        .str.lower()
    )

    comahue[["first_name", "last_name"]] = comahue["full_name"].str.split(
        " ", expand=True
    )
    comahue["age"] = comahue["edad"].apply(age)
    comahue["inscription_date"] = pd.to_datetime(
        comahue["inscription_date"], format="%Y-%m-%d"
    )
    comahue["inscription_date"] = comahue["inscription_date"].astype(str)
    comahue["age"] = comahue["age"].astype(int)

    comahue_loc = pd.merge(
        comahue, location, left_on="postal_code", right_on="codigo_postal", how="left"
    ).reset_index()
    comahue_loc = comahue_loc.rename(columns={"localidad": "location"})
    comahue_loc["university"] = (
        comahue_loc["university"]
        .str.strip()
        .str.lower()
        .str.replace("-", "")
        .str.replace("_", "")
    )
    comahue_loc["career"] = (
        comahue_loc["career"]
        .str.strip()
        .str.lower()
        .str.replace("-", "")
        .str.replace("_", "")
    )
    comahue_loc["location"] = (
        comahue_loc["location"]
        .str.strip()
        .str.lower()
        .str.replace("-", "")
        .str.replace("_", "")
    )
    comahue_loc["email"] = (
        comahue_loc["email"]
        .str.strip()
        .str.lower()
        .str.replace("-", "")
        .str.replace("_", "")
    )
    comahue_loc["gender"] = (
        comahue_loc["gender"]
        .str.strip()
        .str.replace("M", "male")
        .str.replace("F", "female")
        .str.lower()
    )
    comahue_loc["postal_code"] = comahue_loc["postal_code"].astype(str)

    comahue_final = comahue_loc[
        [
            "university",
            "career",
            "inscription_date",
            "first_name",
            "last_name",
            "gender",
            "age",
            "postal_code",
            "location",
            "email",
        ]
    ]

    comahue_final.to_csv(comahue_output_path, sep="\t", index=False)


def process_delsalvador(delsalvador_input_path, delsalvador_output_path):
    """Process the delsalvador file

    Parameters
    ----------
    delsalvador_input_path: str
        str with the location of the files that are going to be processed
        Example:
            "dags/files/delsalvador_university.csv"
    comahue_output_path: str
        str with the location of the txt that will be generated after processing the input files
        Example:
            "dags/files/output/delsalvador.txt"

    Returns
    -------
    None
    """
    url = URL_LOCATION
    path = URL_GOOGLE_EXPORT + url.split("/")[-2]
    location = pd.read_csv(path)
    delsalvador = pd.read_csv(delsalvador_input_path)

    # DEL SALVADOR PROCESSING

    delsalvador["university"] = (
        delsalvador["university"].str.replace("_", " ").str.lower().str.strip()
    )
    delsalvador["career"] = (
        delsalvador["career"].str.replace("_", " ").str.lower().str.strip()
    )
    delsalvador["full_name"] = (
        delsalvador["full_name"].str.replace("_", " ").str.strip()
    )
    delsalvador["location"] = delsalvador["location"].str.replace("_", " ").str.strip()
    delsalvador["email"] = delsalvador["email"].str.lower().str.strip()
    delsalvador["gender"] = (
        delsalvador["gender"]
        .str.strip()
        .str.replace("M", "male")
        .str.replace("F", "female")
        .str.lower()
    )

    delsalvador["full_name"] = (
        delsalvador["full_name"]
        .str.lstrip("DR.")
        .str.lstrip("MR.")
        .str.lstrip("MRS.")
        .str.lstrip("MISS.")
        .str.rstrip("DDS")
        .str.rstrip("IV")
        .str.rstrip("PH")
        .str.rstrip("JR.")
        .str.rstrip("MD")
        .str.rstrip("DVM")
        .str.strip()
        .str.lower()
    )

    delsalvador[["first_name", "last_name"]] = delsalvador["full_name"].str.split(
        " ", expand=True
    )

    delsalvador["inscription_date"] = pd.to_datetime(
        delsalvador["inscription_date"], format="%d-%b-%y"
    )
    delsalvador["edad"] = pd.to_datetime(delsalvador["edad"], format="%d-%b-%y")

    delsalvador["inscription_date"] = delsalvador["inscription_date"].dt.strftime(
        "%Y-%m-%d"
    )
    delsalvador["inscription_date"] = delsalvador["inscription_date"].astype(str)

    delsalvador["edad"] = delsalvador["edad"].dt.strftime("%Y-%m-%d")
    delsalvador["edad"] = delsalvador["edad"].astype(str)

    delsalvador["age"] = delsalvador["edad"].apply(age)

    # The lambda function performs 100 plus the negative ages that were obtained in the transformation in order to obtain valid ages.
    delsalvador["age"] = delsalvador["age"].apply(lambda x: (100 + x) if x < 0 else x)
    delsalvador["age"] = delsalvador["age"].astype(int)

    delsalvador_loc = pd.merge(
        delsalvador, location, left_on="location", right_on="localidad", how="left"
    ).reset_index()
    delsalvador_loc = delsalvador_loc.rename(columns={"codigo_postal": "postal_code"})
    delsalvador_loc["location"] = delsalvador_loc["location"].str.lower()
    delsalvador_loc["postal_code"] = delsalvador_loc["postal_code"].astype(str)

    delsalvador_final = delsalvador_loc[
        [
            "university",
            "career",
            "inscription_date",
            "first_name",
            "last_name",
            "gender",
            "age",
            "postal_code",
            "location",
            "email",
        ]
    ]

    delsalvador_final.to_csv(delsalvador_output_path, sep="\t", index=False)
