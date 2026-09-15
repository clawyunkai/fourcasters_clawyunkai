from prefect import flow, task

@task(retries=3, retry_delay_seconds=5)
def recuperer_donnees():
    print("Je récupère des données...")
    return [1, 2, 3, 4, 5]

@task
def traiter(donnees):
    total=sum(donnees)
    print(f"Total calculé : {total}")

@flow(name="mon-premier-flow")
def mon_flow():
    donnees =recuperer_donnees()
    traiter(donnees)

if __name__=="__main__":
    mon_flow()