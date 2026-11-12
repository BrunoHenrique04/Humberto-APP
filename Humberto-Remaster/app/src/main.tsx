import { useEffect, useState, type FormEvent } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Station = {
  station_id: number;
  name: string;
  line: string;
  clear_sidewalk_pct: number | null;
  valid_clear_area_pct: number | null;
};
type Route = {
  id: string;
  transfers: number;
  stops: number;
  evidenceStatus: string;
  legs: { lineId: number; stations: Station[] }[];
};
type Comparison = { version: string; options: Route[]; radiusM: number };
type Line = {
  line_id: number;
  line: string;
  mode: string;
  stations: number;
  stations_with_environment_data: number;
  mean_clear_sidewalk_pct: number | null;
};

const number = new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 1 });
const percentage = (value: number | null) => value === null ? 'Sem dados' : number.format(value) + '%';
const novoGeoSampa = 'https://novogeosampa.prefeitura.sp.gov.br/#map=18.15/-23.62146/-46.62761/0&layers=103/100/100/&basemap=5/100/100';

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(path, options);
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.message || 'Não foi possível consultar o Humberto.');
  }
  return response.json();
}

function App() {
  const [direction, setDirection] = useState('forward');
  const [preference, setPreference] = useState('fewer_transfers');
  const [comparison, setComparison] = useState<Comparison | null>(null);
  const [lines, setLines] = useState<Line[]>([]);
  const [lineError, setLineError] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    request<{ items: Line[] }>('/api/lines')
      .then(data => setLines(data.items))
      .catch((cause: Error) => setLineError(cause.message));
  }, []);

  async function compare(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError('');
    setComparison(null);
    const forward = direction === 'forward';
    try {
      const result = await request<Comparison>('/api/routes/compare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          origin: forward ? 'Luz' : 'Paraíso',
          destination: forward ? 'Paraíso' : 'Luz',
          preference
        })
      });
      setComparison(result);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Falha ao consultar as rotas.');
    } finally {
      setBusy(false);
    }
  }

  function speak(route: Route) {
    const lines = route.legs.map(leg => `Linha ${leg.lineId} ${leg.stations[0].line}`).join(', depois ');
    const text = `${lines}. ${route.stops} paradas, ${route.transfers} baldeações. A acessibilidade dos trechos aguarda verificação.`;
    const utterance = new SpeechSynthesisUtterance(text);
    utterance.lang = 'pt-BR';
    window.speechSynthesis.cancel();
    window.speechSynthesis.speak(utterance);
  }

  return <main>
    <header>
      <p className="eyebrow">Mobilidade acessível · São Paulo</p>
      <h1>Humberto</h1>
      <p>Compare trajetos do piloto e consulte dados do entorno das estações.</p>
    </header>

    <section aria-labelledby="routes">
      <h2 id="routes">Compare trajetos</h2>
      <form onSubmit={compare}>
        <label>Trajeto
          <select value={direction} onChange={event => setDirection(event.target.value)}>
            <option value="forward">Luz → Paraíso</option>
            <option value="reverse">Paraíso → Luz</option>
          </select>
        </label>
        <label>Priorizar
          <select value={preference} onChange={event => setPreference(event.target.value)}>
            <option value="fewer_transfers">Menos baldeações</option>
            <option value="fewer_stops">Menos paradas</option>
          </select>
        </label>
        <button disabled={busy}>{busy ? 'Comparando…' : 'Comparar'}</button>
      </form>
      {error && <p role="alert">{error}</p>}
      <p className="status" role="status">{comparison && `${comparison.options.length} alternativas. Dados Gold ${comparison.version}.`}</p>
      {comparison && <p className="note">O indicador de calçada sem obstáculos resume os setores do IBGE que interceptam o entorno de {comparison.radiusM} m da estação. Piso tátil, travessias e condições do trajeto aguardam vistoria.</p>}
      {comparison?.options.map(route => <article key={route.id}>
        <div className="route-heading">
          <div><h3>{route.legs.map(leg => `Linha ${leg.lineId} ${leg.stations[0].line}`).join(' + ')}</h3>
            <p>{route.stops} paradas · {route.transfers} baldeação(ões)</p></div>
          {'speechSynthesis' in window && <button className="secondary" onClick={() => speak(route)}>Ouvir resumo</button>}
        </div>
        {route.legs.map(leg => <details key={leg.lineId}>
          <summary>Estações da linha {leg.lineId}</summary>
          <ol>{leg.stations.map(station => <li key={station.station_id}>
            {station.name} <span>{percentage(station.clear_sidewalk_pct)}</span>
          </li>)}</ol>
        </details>)}
      </article>)}
    </section>

    <section aria-labelledby="lines">
      <h2 id="lines">Indicadores por linha</h2>
      <p>Média dos indicadores de calçada sem obstáculos nas estações com dados.</p>
      {lineError && <p role="alert">{lineError}</p>}
      <div className="table-wrap"><table>
        <thead><tr><th scope="col">Linha</th><th scope="col">Estações com dados</th><th scope="col">Média</th></tr></thead>
        <tbody>{lines.map(line => <tr key={line.line_id}>
          <th scope="row">{line.line_id} · {line.line}</th>
          <td>{line.stations_with_environment_data} de {line.stations}</td>
          <td>{percentage(line.mean_clear_sidewalk_pct)}</td>
        </tr>)}</tbody>
      </table></div>
      <p><a href={novoGeoSampa}>Abrir o mapa no Novo GeoSampa</a></p>
    </section>
  </main>;
}

createRoot(document.getElementById('root')!).render(<App />);
