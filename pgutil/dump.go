package pgutil

import (
	"fmt"
	"os"
	"os/exec"
)

func DumpTables(database, user, password, host string, port int, outputDir string, tables ...string) ([]string, error) {
	files := []string{}
	for _, table := range tables {
		// Define el nombre del archivo de salida
		outputFile := fmt.Sprintf("%s/%s.sql", outputDir, table)

		// Configura el comando pg_dump
		cmd := exec.Command(
			"pg_dump",
			"-U", user, // Usuario de la base de datos
			"-h", host, // Host de la base de datos
			"-p", fmt.Sprintf("%d", port),
			"-d", database, // Nombre de la base de datos
			"-t", table, // Dump solo esta tabla
			"--data-only",
			"-f", outputFile, // Archivo de salida
		)

		// Pasar la contraseña a pg_dump usando la variable de entorno
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", password))

		// Ejecutar el comando
		fmt.Printf("Dumping table %s to file %s\n", table, outputFile)
		if err := cmd.Run(); err != nil {
			return files, fmt.Errorf("error dumping table %s: %w", table, err)
		}
		files = append(files, outputFile)
	}
	return files, nil
}
