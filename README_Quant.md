# The Quantsplainer

## Background
Quarterly Earnings Calls provide a forum for company management to summarise their recent performance
and answer questions from all interested parties (institutional investors, analysts, etc.) about the fundamental health and future prospects of the company.

Fundamental analysts and investors aren't the only ones listening to calls. Over the last 5+ years, text-mining models (often deployed by "quants") have joined the conversation.
While I have yet to hear a robot ask a question, [I have first hand experience](https://drive.google.com/file/d/0B0vv_sy7hUb0T0xhUU1rbmhrWE5vX01xa0hTUGdtTlFDOThj/view) documenting how 
effectively a well-trained machine can tease out subtle, yet economically valuable patterns in the dialogue. 

Thousands of conference calls occur worldwide every quarter, and even the most accomplished analysts have finite capacity
to absorb and process information consistently and systematically. It would take one
person more than 700 days—24 hours a day—to listen to a year’s worth of earnings calls.

While their approaches to "note-taking" may vary greatly, quants and fundamental analysts share a strikingly similar motivation: *calls provide valuable context beyond the numbers.*

## Source Data: [www.fool.com](https://www.fool.com/earnings-call-transcripts/?page=1)
As far as I can tell, there are two main publicly available sources for conference call transcripts: [Seeking Alpha](https://seekingalpha.com) and [Motley Fool](https://fool.com/earnings-call-transcripts/).
Seeking Alpha has a great history, but is behind a paywall and the data isn't too neatly organized. Motley Fool has a short-ish history (2017-Present), but the data is free and clean, and so we have our winner. :)

## Quantitative Use-Cases
This list is intended to be representive, but not exaustive, of the sorts of opportunities that exist across the data set.
1. **Sentiment Analysis** - is managmeent optimistic or pessimistic about their future?
2. **Detection of Managment Manipulation** - i.e. the quantification of candor and other red flags
3. **Identification and Analysis of Thematic Trends** - what topics and themes are gaining/losing prominence across the corpus?
4. **Whatever your Quant+NLP heart desires** - ultimately, this is a data-set not stock-picking model. Like any other model input (cash flows, income statements, etc.),  *it's only as valuable as how thoughfully and uniqely it's deployed into the investment process.* God speed and good luck.
