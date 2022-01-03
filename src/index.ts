import { avroToTypeScript, RecordType } from 'avro-typescript';
import axios from 'axios';
import fs from 'fs';

type Config = SchemaRegistryConnectionConfig & {
  outputPath?: string;
};

type SchemaRegistryConnectionConfig = {
  url: string;
  headers?: Record<string, string>;
};

export async function generateSchema({ url, headers, outputPath }: Config) {
  const { data: subjects } = await axios.get<string[]>(`${url}/subjects`, {
    headers,
  });

  const tsTypes = (
    await Promise.all<string>(
      subjects
        .map(
          async (subject: string) =>
            (
              await axios.get<{ id: number }>(
                `${url}/subjects/${subject}/versions/latest`,
                { headers }
              )
            ).data.id
        )
        .map(
          async (id: Promise<number>) =>
            (
              await axios.get<{ schema: string }>(
                `${url}/schemas/ids/${await id}`,
                { headers }
              )
            ).data.schema
        )
    )
  )
    .map(schemaString => JSON.parse(schemaString) as RecordType)
    .map(schema => avroToTypeScript(schema))
    .map(tsSchema => tsSchema.split('\n\n'))
    .flat()
    .sort((a, b) => a.split(' ')[2].localeCompare(b.split(' ')[2]))
    .join('\n');

  fs.writeFile(outputPath ? outputPath : './src/kafkaTypes.ts', tsTypes, () =>
    console.log('✨ Typescript types generated from schema registry.')
  );
}
