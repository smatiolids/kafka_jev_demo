# Classificando e roteando eventos em tempo real com Kafka Connect e JEV

![Fluxo de eventos entrando em um motor de decisão e sendo distribuídos entre diferentes rotas](images/kafka-jev-hero.png)

Se você já precisou utilizar LLMs em aplicações de tempo real orientadas a eventos, caso de uso principal do Kafka, sabe que esbarramos em algumas dificuldades, a saber: Alta latência, alucinações, formato de saída incerto e custo.

Sim, é verdade que todos estes pontos vêm evoluindo desde então, mas ainda que tecnicamente seja viável, para uma grande parcela de casos usar LLMs com eventos não é a maneira mais eficiente.

> O grande objetivo de se utilizar IA em tempo real é **automatizar decisões.**

Eventualmente iremos gerar uma resposta a um cliente, criar um ticket, ações que envolvem gerar texto. Mas, antes disso, houve uma decisão. É um caso crítico? É uma falha? É urgente? Parece ser uma fraude? Qual time é responsável?

É na automação destas decisões, aparentemente pequenas, que se escalam processos de negócio, melhora a experiência do cliente, otimiza o esforço.

Com o surgimento do JEV, modelo de decisão com latência e custo reduzido, vi o complemento ideal para ampliar o que podemos fazer com o Kafka e IA Generativa. Por isto, criei um connector que integra estas tecnologias.

Então neste artigo vamos:

- Consumir tickets de um tópico Kafka.
- Classificá-los com JEV.
- Enriquecer cada evento com as decisões.
- Roteá-los para tópicos diferentes.
- Entender o custo e latência atingidos.

## JEV

Em setembro de 2026 foi anunciado o JEV, família de modelos criada pela TypeSafe.ai, que tem como objetivo gerar decisões (e não texto) com alguns diferenciais relevantes para uso com eventos:

- Latência: Como ele não precisa gerar tokens que dependem dos tokens anteriores, ele pode paralelizar o processamento de cada questão. Com isso, a latência é muito menor comparadada aos LLMs.
- Menos alucinações: Não só as questões são processadas em paralelo, como cada questão é classificada várias vezes paralelamente para reduzir variações e chegar a uma classificacão média. Ainda não é determinístico, mas está menos sujeito a alucinações
- Estrutura: O formato de saída não é produzido via geração de tokens, logo não varia. Sabemos o que esperar com retorno do modelo e não é necessário validá-lo.
- Custo: Cobrado somente por tokens de entrada, possui um custo de USD 0.042 por milhão de tokens. Para comparação, o modelo gpt-5-nano, o mais barato atualmente, custa USD 0.05 por milhão de tokens de entrada, mas também cobra USD 0,20 por milhão de tokens de saída.

Com estes atributos, especialmente o baixo custo e baixa latência, o JEV se torna uma ótima opção para conectarmos uma modelo de decisão a eventos recebidos pelo Kafka. Para isto, chegamos ao...

## Kafka-JEV-Connector

Indo direto ao ponto, o Kafka-JEV-Connector consome mensagens de tópicos determinados, monta um contexto baseado no payload das mensagens e submete ao JEV, que retorna ao conector. Este resultado é então encaminhado aos tópicos de saída, assim como a tópico DLQ em caso de falhas. O tópico destino pode ser um específco, como pode ser determinado pelo JEV, ou seja, o JEV age como um roteador de mensagens.

![Arquitetura do Kafka-JEV-Connector entre o tópico de entrada, a API JEV, os tópicos de saída e a DLQ](images/architecture-overview.svg)

*Visão simplificada do fluxo entre a origem, o modelo e os tópicos de destino.*

O conector é open source e está disponível no repositório [kafka-jev-connector](https://github.com/smatiolids/kafka-jev-connector).

## Mão na massa

Todo o cenário, incluindo configurações, ambiente local e scripts, está no repositório [kafka_jev_demo](https://github.com/smatiolids/kafka_jev_demo).

O conector é open source e está disponível no repositório [kafka-jev-connector](https://github.com/smatiolids/kafka-jev-connector).

Vamos enviar tickets de suporte de um arquivo CSV para o tópico `messages`. O conector irá avaliá-los e publicar o resultado em tópicos iniciados por `messages_jev`.

Antes disso, precisamos definir duas coisas: quais perguntas queremos fazer e quais dados do evento serão utilizados para respondê-las.

### Configurando as Questions

As perguntas estão em `config/questions.json`. Cada propriedade representa uma Question, e seu nome se torna o identificador utilizado nas respostas e nas regras de roteamento.

Nosso exemplo possui seis perguntas:

- O ticket precisa de intervenção humana?
- Existe risco de cancelamento?
- Qual departamento deve atendê-lo?
- Qual a intenção do cliente?
- Qual a urgência?
- Qual o nível de insatisfação?

Para respondê-las, utilizamos três tipos: `noul`, `choice` e `score`.

#### Noul

`noul` representa uma decisão entre verdadeiro e falso, mas retorna um valor contínuo entre `0` e `1`.

```json
"requires_human": {
  "type": "noul",
  "instructions": "Does this ticket require human intervention?",
  "criteria": {
    "true": "The issue cannot be safely resolved by an automated response or requires a privileged action.",
    "false": "The issue can follow the normal automated flow."
  }
}
```

Além da pergunta, explicamos ao modelo o que verdadeiro e falso significam dentro do nosso processo.

#### Choice

`choice` é utilizado quando temos um conjunto fechado de respostas. Para `department`, o JEV deverá escolher exatamente uma das áreas definidas em `criteria`.

```json
"department": {
  "type": "choice",
  "instructions": "Which team should be primarily responsible for this ticket?",
  "criteria": {
    "billing": "Charges, payments, invoices, or refunds.",
    "technical_support": "Technical failures, access, or product functionality.",
    "account_security": "Locked account, credentials, fraud, or suspicious access.",
    "retention": "Cancellation, serious dissatisfaction, or risk of losing the customer.",
    "general_support": "Matters that do not clearly belong to the other teams."
  }
}
```

Estas chaves fazem parte do contrato e podem ser utilizadas diretamente no nome do tópico de destino.

#### Score

`score` posiciona o evento dentro de uma escala ordenada. A ordem dos critérios é importante.

```json
"urgency": {
  "type": "score",
  "instructions": "Rate the operational urgency of the ticket.",
  "criteria": [
    "Can wait for the normal flow.",
    "Should be handled this week.",
    "Should be handled today.",
    "Requires immediate attention to avoid significant impact."
  ]
}
```

Com quatro critérios, os níveis vão de `0` a `3`. O JEV também retorna um score contínuo, permitindo que a política utilize o nível ou aplique um limiar próprio.

### Adicionando as Questions ao conector

O Kafka Connect recebe propriedades simples. Por isso, o objeto de Questions precisa ser serializado em `jev.questions`.

Após editar `questions.json`, execute:

```bash
python3 config/set_questions.py
```

O script atualiza `jev.questions` em `config/connector.json`.

### Criando o State

Se as Questions definem o que queremos saber, o State define o contexto utilizado pelo JEV para responder.

O conector pode enviar o valor completo do evento ou montar este contexto dinamicamente.

#### Full Value

Com `FULL_VALUE`, todo o valor da mensagem é enviado ao JEV:

```json
{
  "state.mode": "FULL_VALUE"
}
```

É a opção mais simples, mas pode incluir dados irrelevantes ou sensíveis. Para selecionar somente os campos necessários, utilizamos `TEMPLATE`.

#### Template

O template da demonstração é:

```json
{
  "state.mode": "TEMPLATE",
  "state.template": "Ticket: ${key}\nSubject: ${value:/subject:-(no subject)}\nMessage: ${value:/body:-(no message)}"
}
```

Para cada mensagem, o conector substitui as expressões pelos valores do registro:

```text
Ticket: ticket-42
Subject: Payment failed
Message: My card was charged twice
```

![Construção do State a partir da chave, dos campos do evento e de um template](images/state-template.svg)

As expressões mais comuns são:

| Expressão | Conteúdo |
| --- | --- |
| `${value}` | Valor completo da mensagem |
| `${key}` | Chave completa |
| `${value:/customer/name}` | Campo selecionado com JSON Pointer |
| `${metadata:topic}` | Tópico de origem |

Também podemos utilizar headers, outras informações de metadata e valores padrão. A referência completa está na documentação do conector.

Sem um valor padrão, uma referência ausente é uma falha permanente e o registro segue para a DLQ.

### Configurando o conector

Com Questions e State definidos, precisamos apenas das propriedades específicas do conector:

```json
{
  "connector.class": "io.github.smatiolids.kafkajev.JevSinkConnector",
  "topics": "messages",
  "output.topic": "messages_jev",
  "errors.deadletter.topic": "messages_jev_dlq",
  "jev.api.key": "${env:TYPESAFE_API_KEY}",
  "jev.model": "jev-latest",
  "question.set.id": "message_classification/v1",
  "jev.questions": "<questions.json serializado>",
  "state.mode": "TEMPLATE",
  "state.template": "Ticket: ${key}\nSubject: ${value:/subject:-(no subject)}\nMessage: ${value:/body:-(no message)}"
}
```

`topics` define a entrada, `errors.deadletter.topic` define a DLQ e `output.topic` define o destino das mensagens processadas.

### Roteando os eventos

O projeto possui um arquivo `connector-local*.json` para cada política. Para alternar entre elas, alteramos `CONNECTOR_CONFIG_FILE` no `.env`.

O conector não cria tópicos. Todos os destinos devem existir antes de sua inicialização.

![Comparação entre tópico fixo, roteamento por Choice, condição e nível de Score](images/routing-policies.svg)

#### Tópico fixo

```json
"output.topic": "messages_jev"
```

Todos os resultados são publicados em `messages_jev`. É a melhor opção quando outro consumidor fará o roteamento ou quando queremos manter um fluxo único.

#### Roteamento por Choice

```json
"output.topic": "messages_jev_${answers.department.choice}"
```

Se `department` resultar em `billing`, o evento vai para `messages_jev_billing`. Esta opção é útil quando cada alternativa representa uma equipe ou workflow.

#### Roteamento por Noul

```json
"output.topic": "messages_jev_${answers.requires_human.noul > 0.8; \"to_human\", \"to_agent\"}"
```

Valores maiores que `0.8` seguem para `messages_jev_to_human`; os demais seguem para `messages_jev_to_agent`.

O limiar é uma regra de negócio. Podemos torná-lo mais ou menos conservador conforme o impacto de uma decisão incorreta.

#### Roteamento por Score

Podemos comparar o valor contínuo:

```json
"output.topic": "messages_jev_${answers.urgency.score > 2; \"urgent\", \"normal\"}"
```

Ou utilizar diretamente o nível da escala:

```json
"output.topic": "messages_jev_dissatisfaction_${answers.customer_dissatisfaction.level}"
```

O valor permite aplicar um limite próprio. O nível preserva as categorias definidas nos critérios da Question.

Em todos os casos, uma avaliação produz uma única publicação. O conector seleciona um destino; ele não distribui a mesma resposta para vários tópicos.

### Executando localmente

Baixe o plugin, crie o arquivo `.env` e informe sua chave da TypeSafe. A configuração completa está no [README](../README.md).

As propriedades que iremos alternar são:

```dotenv
TYPESAFE_API_KEY=<sua chave da TypeSafe>
CONNECTOR_CONFIG_FILE=./config/connector-local.json
```

Inicie Kafka e Kafka Connect:

```bash
docker compose -f compose.kafka.yaml -f compose.yaml up -d --build --wait
```

Verifique o conector:

```bash
curl --fail --silent localhost:8083/connectors/jev/status
```

Instale as dependências e envie dez tickets:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python helper/send_message.py --limit 10
```

Acompanhe os resultados:

```bash
.venv/bin/python helper/receive_messages.py --from-beginning --pretty
```

### Resultado da execução

Para validar o cenário, executamos os 4.000 tickets em cada uma das cinco políticas. Isto produziu 20.000 registros enriquecidos, todos com o modelo resolvido `jev-1.14.0`.

![Resumo da execução com avaliações, latência, custo, DLQ e distribuições de roteamento](images/execution-results.svg)

Os resultados completos estão em [`docs/results.txt`](results.txt) e podem ser gerados novamente com:

```bash
.venv/bin/python helper/summarize_results.py
```

#### O que os roteamentos mostraram

Três resultados ajudam a entender o comportamento das políticas:

- `technical_support` recebeu 80,9% dos tickets no roteamento por `choice`.
- O limiar `requires_human > 0.8` enviou 1.797 tickets para humanos e 2.203 para o agente.
- A regra `urgency.score > 2` classificou 1.573 tickets, ou 39,3%, como urgentes.

Cada cenário fez uma nova avaliação. Como o resultado não é determinístico, pequenas diferenças entre a saída fixa e as saídas roteadas são esperadas.

O exemplo de `requires_human` mostra a separação entre modelo e regra de negócio. O JEV produz o valor; nós decidimos a partir de qual ponto ele muda o caminho do evento.

#### Latência, escala e custo

Nas 20.000 avaliações, a latência média da chamada ao JEV foi de 210 ms. O p50 foi de 201 ms, o p95 de 279 ms e a chamada mais lenta levou 1.518 ms.

Todas as avaliações terminaram na primeira tentativa e nenhum registro foi enviado à DLQ.

A execução consumiu 20.002.245 tokens de entrada e custou aproximadamente USD 0,84. Uma passagem pelos 4.000 tickets custaria cerca de USD 0,17 nas mesmas condições.

Mantido o mesmo perfil, o custo estimado é de USD 0,042 por mil avaliações e USD 42,00 por milhão.

> Para casos de tempo real, este tipo de modelo combina baixa latência por evento, capacidade de processar decisões em volume e custo previsível para escalar.

Não se trata de um benchmark universal. Ainda assim, a execução mostra bem o perfil: 20.000 decisões estruturadas, p95 de 279 ms, nenhuma tentativa adicional, nenhuma DLQ e custo inferior a um dólar.

#### O que o JEV respondeu

Entre as seis Questions, três resultados se destacaram:

- `department`: 80,9% `technical_support`, com confiança média de `0,93`.
- `requires_human`: valor médio de `0,74`; 90,1% ficaram acima de `0,5`.
- `cancellation_risk`: média de `0,04`; apenas 1,6% ficaram acima de `0,5`.

Estes números ajudam a avaliar se as Questions e seus critérios representam o processo que queremos automatizar. A distribuição completa permanece disponível em `results.txt`.

## Conclusões

Minha intenção ao criar este conector era abrir caminho para um novo jeito de usar modelos.

Depois de entender o que é possível com o JEV e modelos semelhantes, ficou claro que sua combinação com Kafka poderia destravar casos de uso que, com LLMs, são lentos ou caros para executar em escala.

Se usar o conector, star the repo!
