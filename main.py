from neo4j import GraphDatabase
import json

class GrafoPokemon:
    def __init__(self, uri, usuario, senha):
        print("Inicializando conexão com o banco de dados")
        self.driver = GraphDatabase.driver(uri, auth=(usuario, senha))
        print("Conexão estabelecida com sucesso!")

    def fechar(self):
        print("Fechando a conexão com o banco de dados...")
        self.driver.close()
        print("Conexão fechada.")

    def carregar_dados(self, arquivo_json):
        print(f"Lendo dados do arquivo {arquivo_json}...")
        with open(arquivo_json, 'r', encoding='utf-8') as f:
            dados = json.load(f)
        print(f"{len(dados)} Pokémons carregados do JSON.")
        with self.driver.session() as session:
            print("Limpando a base de dados")
            session.execute_write(self._limpar_base)
            print("Base de dados limpa")
            print("Criando índices")
            session.execute_write(self._criar_indices)
            print("Índices criados.")
            print("Inserindo Pokémons no banco de dados")
            for idx, pokemon in enumerate(dados, start=1):
                print(f"Inserindo Pokémon {idx}/{len(dados)}: {pokemon['nome']}")
                session.execute_write(self._criar_pokemon, pokemon)
            print("Todos os Pokémons foram inseridos.")

    @staticmethod
    def _limpar_base(tx):
        tx.run("MATCH (n) DETACH DELETE n")

    @staticmethod
    def _criar_indices(tx):
        tx.run("CREATE INDEX IF NOT EXISTS FOR (p:Pokemon) ON (p.nome)")
        tx.run("CREATE INDEX IF NOT EXISTS FOR (t:Tipo) ON (t.nome)")
        tx.run("CREATE INDEX IF NOT EXISTS FOR (h:Habilidade) ON (h.nome)")

    @staticmethod
    def _criar_pokemon(tx, pokemon):
        tx.run("""
        MERGE (p:Pokemon {numero: $numero})
        SET p.nome = $nome,
            p.altura_cm = $altura_cm,
            p.peso_kg = $peso_kg,
            p.url = $url
        """, numero=pokemon['numero'], nome=pokemon['nome'], altura_cm=pokemon['altura_cm'],
             peso_kg=pokemon['peso_kg'], url=pokemon['url'])

        tipos = [t.strip() for t in pokemon['tipos'].split(',')]
        for tipo in tipos:
            tx.run("""
            MERGE (t:Tipo {nome: $tipo})
            MERGE (p:Pokemon {numero: $numero})
            MERGE (p)-[:TEM_TIPO]->(t)
            """, tipo=tipo, numero=pokemon['numero'])

        for habilidade in pokemon.get('habilidades', []):
            tx.run("""
            MERGE (h:Habilidade {nome: $nome})
            SET h.descricao = $descricao,
                h.efeito = $efeito,
                h.url = $url
            MERGE (p:Pokemon {numero: $numero})
            MERGE (p)-[:TEM_HABILIDADE]->(h)
            """, nome=habilidade['nome'], descricao=habilidade['desc'], efeito=habilidade['efeito'],
                 url=habilidade['url'], numero=pokemon['numero'])

        for evolucao in pokemon.get('proximas_evolucoes', []):
            tx.run("""
            MERGE (p1:Pokemon {numero: $numero1})
            MERGE (p2:Pokemon {numero: $numero2})
            MERGE (p1)-[:EVOLUI_PARA]->(p2)
            """, numero1=pokemon['numero'], numero2=evolucao['numero'])


if __name__ == "__main__":
    uri = "neo4j+s://098f3058.databases.neo4j.io"
    usuario = "neo4j"
    senha = "AiUWdToI9Jhaa42DK1DPf7Hym_PEkyNAPw01-v0kdcg"

    arquivo_json = "pokemons.json"

    grafo_pokemon = GrafoPokemon(uri, usuario, senha)
    print("Carregando dados no banco de dados...")
    grafo_pokemon.carregar_dados(arquivo_json)
    print("Dados carregados com sucesso!")
    grafo_pokemon.fechar()
    print("Operações concluídas com sucesso.")
