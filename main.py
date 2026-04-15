import os
import glob
import pandas as pd
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Função para ler um único CSV
def read_csv(file_path):
    try:
        return pd.read_csv(file_path, encoding='utf-8')
    except Exception as e:
        print(f"Erro ao ler {file_path}: {e}")
        return pd.DataFrame()

# Função para consolidar CSVs serialmente
def consolidate_serial(csv_files):
    all_data = []
    for file in csv_files:
        df = read_csv(file)
        if not df.empty:
            all_data.append(df)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

# Função para consolidar CSVs paralelamente
def consolidate_parallel(csv_files):
    all_data = []
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        futures = {executor.submit(read_csv, file): file for file in csv_files}
        for future in as_completed(futures):
            df = future.result()
            if not df.empty:
                all_data.append(df)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

# Função principal
def main():
    folder_path = './Base de Dados'
    
    # Validar se a pasta existe
    if not os.path.exists(folder_path):
        print(f"Erro: A pasta '{folder_path}' não existe.")
        return
    
    # Encontrar todos os CSVs
    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
    if not csv_files:
        print(f"Nenhum arquivo CSV encontrado em '{folder_path}'.")
        return
    
    print(f"Encontrados {len(csv_files)} arquivos CSV.")
    
    # Solicitar município ao usuário
    municipio = input("Digite o nome do município: ").strip().upper()
    
    # Versão Serial
    print("Executando versão serial...")
    start_time = time.time()
    df_serial = consolidate_serial(csv_files)
    if df_serial.empty:
        print("Nenhum dado foi consolidado.")
        return
    
    filtered_serial = df_serial[df_serial['municipio_oj'].str.upper() == municipio]
    serial_time = time.time() - start_time
    print(f"Tempo de execução serial: {serial_time:.2f} segundos")
    print(f"Ocorrências encontradas (serial): {len(filtered_serial)}")
    
    # Versão Paralela
    print("Executando versão paralela...")
    start_time = time.time()
    df_parallel = consolidate_parallel(csv_files)
    if df_parallel.empty:
        print("Nenhum dado foi consolidado.")
        return
    
    filtered_parallel = df_parallel[df_parallel['municipio_oj'].str.upper() == municipio]
    parallel_time = time.time() - start_time
    print(f"Tempo de execução paralela: {parallel_time:.2f} segundos")
    print(f"Ocorrências encontradas (paralela): {len(filtered_parallel)}")
    
    # Comparar tempos
    if serial_time > 0:
        speedup = serial_time / parallel_time if parallel_time > 0 else float('inf')
        print(f"Speedup (serial/paralela): {speedup:.2f}x")
    
    # Gerar arquivo .txt (usando dados da versão paralela, assumindo consistência)
    if not filtered_parallel.empty:
        output_file = f"{municipio}.txt"
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"Ocorrências para o município: {municipio}\n")
                f.write(f"Total: {len(filtered_parallel)}\n")
                f.write("\n")
                for _, row in filtered_parallel.iterrows():
                    f.write(str(row.to_dict()) + "\n")
            print(f"Arquivo '{output_file}' gerado com sucesso.")
        except Exception as e:
            print(f"Erro ao gerar arquivo: {e}")
    else:
        print(f"Nenhuma ocorrência encontrada para o município '{municipio}'.")

if __name__ == "__main__":
    main()