import React from 'react';
import { Map } from './components/Map/Map';
import './App.css';

export function App(): React.ReactElement {
    return (
        <div className="App">
            <Map />
        </div>
    );
}

export default App;