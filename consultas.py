from neo4j import GraphDatabase

class ConsultasPokemon:
    def __init__(self, uri, usuario, senha):
        self.driver = GraphDatabase.driver(uri, auth=(usuario, senha))

    def fechar(self):
        self.driver.close()

    def consulta_pokemons_atacantes(self):
        with self.driver.session() as session:
            resultado = session.execute_read(self._consulta_pikachu_fraqueza)
        for registro in resultado:
            print(f"{registro['nome']} - Peso: {registro['peso_kg']} kg")

    @staticmethod
    def _consulta_pikachu_fraqueza(tx):
        consulta = """
        MATCH (pikachu:Pokemon {nome: 'Pikachu'})-[:TEM_TIPO]->(tipo_pikachu)
        WITH COLLECT(DISTINCT tipo_pikachu.nome) AS tipos_pikachu
        MATCH (pokemon:Pokemon)-[:TEM_TIPO]->(tipo_pokemon)
        WHERE tipo_pokemon.nome IN ['Ground'] AND pokemon.peso_kg > 10
        RETURN DISTINCT pokemon.nome AS nome, pokemon.peso_kg AS peso_kg
        """
        resultado = tx.run(consulta)
        return [record for record in resultado]

    def consulta_evolucoes_peso(self):
        with self.driver.session() as session:
            total_segunda = session.execute_read(self._consulta_segunda_evolucao)
            total_terceira = session.execute_read(self._consulta_terceira_evolucao)
        print(f"Número de segundas evoluções que dobram o peso: {total_segunda}")
        print(f"Número de terceiras evoluções que dobram o peso: {total_terceira}")

    @staticmethod
    def _consulta_segunda_evolucao(tx):
        consulta = """
        MATCH (p1:Pokemon)-[:EVOLUI_PARA]->(p2:Pokemon)
        WHERE p2.peso_kg >= 2 * p1.peso_kg
        RETURN COUNT(DISTINCT p2.numero) AS total
        """
        resultado = tx.run(consulta)
        return resultado.single()['total']

    @staticmethod
    def _consulta_terceira_evolucao(tx):
        consulta = """
        MATCH (p1:Pokemon)-[:EVOLUI_PARA]->(p2:Pokemon)-[:EVOLUI_PARA]->(p3:Pokemon)
        WHERE p3.peso_kg >= 2 * p2.peso_kg
        RETURN COUNT(DISTINCT p3.numero) AS total
        """
        resultado = tx.run(consulta)
        return resultado.single()['total']

if __name__ == "__main__":
    uri = "neo4j+s://098f3058.databases.neo4j.io"
    usuario = "neo4j"
    senha = "AiUWdToI9Jhaa42DK1DPf7Hym_PEkyNAPw01-v0kdcg"

    consultas = ConsultasPokemon(uri, usuario, senha)
    consultas.consulta_pokemons_atacantes()
    consultas.consulta_evolucoes_peso()
    consultas.fechar()
    print("Consultas concluídas com sucesso.")
