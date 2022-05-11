import re
from dateutil import parser
from lxml import html
import logging
from foolcalls.decorators import handle_many_elements, handle_one_element
import multiprocessing as mp

log = logging.getLogger(__name__)


# extract the links
@handle_many_elements(error_on_empty=False)
def get_call_urls(html_selector):
    call_urls = html_selector.xpath('.//div[@class = "content-block listed-articles recent-articles m-np"]'
                                    '//div[@class="list-content"]/a/@href')
    return call_urls


# ---------------------------------------------------------------------------
# SELECTORS: FIND CONTAINERS FOR THE VARIOUS ITEMS WE WILL EXTRACT
# ---------------------------------------------------------------------------
@handle_one_element(error_on_empty=True)
def find(parent_element, xpath_str):
    return parent_element.xpath(xpath_str)


@handle_many_elements(error_on_empty=True)
def findall(parent_element, xpath_str, concat_results=False):
    elements = parent_element.xpath(xpath_str)
    if concat_results:
        return html.fromstring(b''.join([html.tostring(el) for el in elements]))
    return elements


def find_containers(html_text):
    html_doc = html.fromstring(html_text)

    publication_info = find(parent_element=html_doc,
                            xpath_str='.//div[@class="author-and-date"]')

    article_header = find(parent_element=html_doc,
                          xpath_str='.//section[@class="usmf-new article-header"]/header')

    article_body = find(parent_element=html_doc,
                        xpath_str='.//section[@class="usmf-new article-body"]/span[@class="article-content"]')

    transcript_header = findall(parent_element=article_body,
                                xpath_str='./h2[text()="Contents:"]/preceding-sibling::p',
                                concat_results=True)

    try:
        pres_elements = findall(parent_element=article_body,
                                xpath_str='./h2[text()[contains(.,"Prepared")]]'
                                          '/following-sibling::h2[text()[contains(.,"Questions")]]'
                                          '/preceding-sibling::p'
                                          '[preceding-sibling::h2[text()[contains(.,"Prepared")]]]')
    except:
        pres_elements = findall(parent_element=article_body,
                                xpath_str='./h2[text()[contains(.,"Prepared")]]'
                                          '/following-sibling::p[strong/text()[contains(.,"Questions")]]'
                                          '/preceding-sibling::p'
                                          '[preceding-sibling::h2[text()[contains(.,"Prepared")]]]')

    try:
        qa_elements_raw = findall(parent_element=article_body,
                                  xpath_str='./h2[text()[contains(.,"Questions")]]'
                                            '/following-sibling::h2[text()[contains(.,"Call")]]'
                                            '/preceding-sibling::p'
                                            '[preceding-sibling::h2[text()[contains(.,"Questions")]]]')
    except:
        qa_elements_raw = findall(parent_element=article_body,
                                  xpath_str='./p[strong/text()[contains(.,"Questions")]]'
                                            '/following-sibling::p[strong/text()[contains(.,"Call")]]'
                                            '/preceding-sibling::p'
                                            '[preceding-sibling::p[strong/text()[contains(.,"Questions")]]]')
    qa_elements = qa_elements_raw[:-1]
    duration_element = qa_elements_raw[-1]

    elements = {'html_doc': html_doc,
                'publication_info': publication_info,
                'article_header': article_header,
                'article_body': article_body,
                'transcript_header': transcript_header,
                'pres': pres_elements,
                'qa': qa_elements,
                'duration': duration_element}

    log.info(f'pid[{mp.current_process().pid}] successfully identified all {len(elements.keys())} selectors/containers')
    print(f'pid[{mp.current_process().pid}] successfully identified all {len(elements.keys())} selectors/containers')
    return elements


# ---------------------------------------------------------------------------
# EXTRACTORS: PARSE AND STRUCTURE SPECIFIC INFORMATION WITHIN CONTAINERS
# ---------------------------------------------------------------------------
def get_publication_metadata(publisher_metadata):
    # author
    publication_author = ' '.join(publisher_metadata.xpath('.//div[@class="author-name"]//text()'))

    # publication time(s)
    # if there was no update, there's just a date (eg 'Jan 27, 2020 at 11:00PM')
    # if there was an update, it looks like this: 'Updated: Feb 5, 2020 at 2:30PM Published: Jan 27, 2020 at 11:00PM'
    pub_time_raw = ' '.join(publisher_metadata.xpath('.//div[@class="publication-date"]//text()'))
    pub_time_cln = re.sub('\s+', ' ', pub_time_raw).strip()

    pub_time_updated = ''.join(re.findall('^Updated: (.*) Published:', pub_time_cln))
    if len(pub_time_updated) > 0:
        pub_time_published = ''.join(re.findall('Published: (.*)$', pub_time_cln))

    else:
        pub_time_published = pub_time_cln
        pub_time_updated = pub_time_published

    metadata = {'publication_author': re.sub('\s+', ' ', publication_author).strip(),
                'publication_time_published': str(parser.parse(pub_time_published)),
                'publication_time_updated': str(parser.parse(pub_time_updated))}
    return metadata


def get_title_metadata(article_header):
    # titles
    call_title = ''.join(article_header.xpath('./h1/text()'))
    call_subtitle = ''.join(article_header.xpath('./h2/text()'))

    # period end in standard format
    mmm_d_yyyy = ''.join(re.findall('period ending ([a-zA-z ]+\\d{1,2}, 20\\d\\d)\.$', call_subtitle))
    try:
        period_end = parser.parse(mmm_d_yyyy).strftime('%Y-%m-%d')
    except Exception as e:
        period_end = ''

    metadata = {'call_title': call_title,
                'call_subtitle': call_subtitle,
                'period_end': period_end}
    return metadata


def get_header_metadata(header_element):
    # name
    company_name = ''.join(header_element.xpath('.//strong/text()'))

    # company id
    fool_company_id = ''.join(header_element.xpath('.//span[@class="ticker"]/@data-id'))

    # ticker/exchange
    tickers = header_element.xpath('.//span[@class="ticker"]/a/text()')
    ticker = tickers[0].split(':')[1] if len(tickers) > 0 else ''
    exchange = tickers[0].split(':')[0] if len(tickers) > 0 else ''
    if len(tickers) > 1:
        log.warning(f'multiple tickers: {",".join(tickers)}')

    # variable length list of metadata
    header_subtext = header_element.xpath('.//text()')
    header_subtext = [text.replace('\xa0', ' ') for text in header_subtext
                      if len(text) > 3 and text not in tickers]

    # short title
    short_title = ''.join([value for value in header_subtext
                           if re.search('(earnings call)|(conference call)|(earnings conference)', value.lower())])

    # extract qtr and year from title
    fiscal_period_qtr, fiscal_period_year = '', ''
    q_yyyy = (''.join(re.findall('Q\\d 20\\d\\d', short_title))).split(' ')
    if len(q_yyyy) == 2:
        fiscal_period_qtr, fiscal_period_year = q_yyyy

    # get date and time in standard format
    call_date = ''.join([parser.parse(value).strftime('%Y-%m-%d')
                         for value in header_subtext if re.search('20\\d\\d.{0,3}$', value)])
    call_time_raw = ''.join([value for value in header_subtext
                             if re.search('^\\d\\d?:\\d\\d', value)])
    try:
        call_time = str(parser.parse(f'{call_date} {call_time_raw}', ignoretz=True))
    except Exception as e:
        log.warning(f'{e}')
        call_time = ''

    metadata = {'ticker': ticker,
                'ticker_exchange': exchange,
                'company_name': company_name,
                'fool_company_id': fool_company_id,
                'fiscal_period_year': fiscal_period_year,
                'fiscal_period_qtr': fiscal_period_qtr,
                'call_short_title': short_title,
                'call_date': call_date,
                'call_time': call_time}

    log.info(f'pid[{mp.current_process().pid}] successfully extracted call-level metadata from transcript')
    print(f'pid[{mp.current_process().pid}] successfully extracted call-level metadata from transcript')
    return metadata


def get_duration_metadata(duration_element):
    duration_text = ''.join(duration_element.xpath('.//text()'))
    return {'duration_minutes': ''.join(re.findall('Duration: (\\d{1,3}) minutes', duration_text))}


def get_participant_metadata(call_statements):
    mgmt_md, mgmt_ids = [], []
    analyst_md, analyst_ids = [], []

    keys = ['speaker', 'role', 'affiliation']

    for this_statement in call_statements:
        if this_statement['statement_type'] in ['A', 'P'] and this_statement['speaker'] not in mgmt_ids:
            mgmt_md.append({k: this_statement[k] for k in keys})
            mgmt_ids.append(this_statement['speaker'])
        elif this_statement['statement_type'] == 'Q' and this_statement['speaker'] not in analyst_ids:
            analyst_md.append({k: this_statement[k] for k in keys})
            analyst_ids.append(this_statement['speaker'])

    call_participant_metadata = {'management': mgmt_md,
                                 'analysts': analyst_md}
    return call_participant_metadata


# TRANSCRIPT STATEMENT EXTRACTORS
def get_statement_data(pres_elements, qa_elements):
    pres_statements = get_statements_by_section(pres_elements,
                                                statement_num_start=1,
                                                section_name='pres')

    qa_statements = get_statements_by_section(qa_elements,
                                              statement_num_start=len(pres_statements) + 1,
                                              section_name='qa')

    output = pres_statements + qa_statements

    log.info(
        f'pid[{mp.current_process().pid}] successfully extracted all statements (and statement-level metadata) from the transcript')
    print(
        f'pid[{mp.current_process().pid}] successfully extracted all statements (and statement-level metadata) from the transcript')
    return output


def get_statements_by_section(transcript_elements, statement_num_start, section_name):
    statement_breakpoints = get_statement_breakpoints(transcript_elements)

    output = []
    for i in range(len(statement_breakpoints) - 1):
        statement_data = {'statement_num': statement_num_start + i, 'section': section_name}  # init dict

        statement_header = transcript_elements[statement_breakpoints[i]]  # element
        statement_metadata = get_statement_metadata(statement_header)  # dict for output
        statement_type = assign_statement_type(statement_metadata, section_name)  # dict for output

        # speaker dialogue (list of different paragraphs)
        paragraphs = transcript_elements[(statement_breakpoints[i] + 1):statement_breakpoints[i + 1]]  # element
        statement_text = {'text': ''.join([''.join(para.xpath('.//text()')).replace('\xa0', ' ')
                                           for para in paragraphs])}

        # combine all dicts, starting with init dict
        statement_data.update(statement_type)
        statement_data.update(statement_metadata)
        statement_data.update(statement_text)

        # update output list
        output.append(statement_data)

    return output


def get_statement_breakpoints(transcript_elements):
    # find headers for each statement block
    # where "statement block" = the beginning of a speaker's dialogue
    statement_header_locs = [i for i, header in enumerate(transcript_elements)
                             if '<strong>' in html.tostring(header, encoding='unicode')]
    statement_breakpoints = statement_header_locs + [len(transcript_elements) - 1]
    return statement_breakpoints


def get_statement_metadata(statement_header_element):
    speaker = ''.join(statement_header_element.xpath('.//strong/text()'))

    speaker_desc = statement_header_element.xpath('.//em/text()')
    speaker_desc = speaker_desc[0] if len(speaker_desc) > 0 else None

    affiliation, role = '', ''
    if speaker_desc is not None:
        speaker_desc = speaker_desc.split(' -- ')
        if len(speaker_desc) == 1:
            role = speaker_desc[0]
            affiliation = ''
        elif len(speaker_desc) == 2:
            affiliation = speaker_desc[0]
            role = speaker_desc[1]

    statement_metadata = {'speaker': re.sub('(\-\-$)|(^\-\-)', '', speaker.strip()).strip(),
                          'role': re.sub('(\-\-$)|(^\-\-)', '', role.strip()).strip(),
                          'affiliation': re.sub('(\-\-$)|(^\-\-)', '', affiliation.strip()).strip()}

    return statement_metadata


def assign_statement_type(statement_metadata, section_name):
    if re.search('(^anal[a-z]{2,4})|([a-z]{2,4}lyst$)', statement_metadata['role'].lower()) and section_name == 'qa':
        statement_type = 'Q'  # question
    elif statement_metadata['speaker'] == 'Operator':
        statement_type = 'O'  # operator
    elif statement_metadata['affiliation'] == '' and section_name == 'pres':
        statement_type = 'P'  # presentation from mgmt
    elif statement_metadata['affiliation'] == '' and section_name == 'qa':
        statement_type = 'A'  # answer from mgmt
    else:
        statement_type = 'U'  # unknown

    return {'statement_type': statement_type}
