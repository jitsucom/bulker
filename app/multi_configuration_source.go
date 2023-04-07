package app

import "reflect"

type MultiConfigurationSource struct {
	configurationSources []ConfigurationSource
	changesChan          chan bool
	closeChan            chan struct{}
}

func NewMultiConfigurationSource(configurationSources []ConfigurationSource) *MultiConfigurationSource {
	mcs := MultiConfigurationSource{configurationSources: configurationSources,
		changesChan: make(chan bool, 1),
		closeChan:   make(chan struct{})}
	// gather signals from all changes channels from configuration sources into one channel
	go func() {
		cases := make([]reflect.SelectCase, len(configurationSources))
		for i, cs := range configurationSources {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cs.ChangesChannel())}
		}
		for {
			select {
			case <-mcs.closeChan:
				return
			default:
				// Use reflect.Select to select from the dynamic numbers of channels
				i, _, ok := reflect.Select(cases)
				if !ok {
					// The chosen channel has been closed, so zero out the channel to disable the case
					cases[i].Chan = reflect.ValueOf(nil)
				} else {
					mcs.changesChan <- true
				}
			}
		}
	}()
	return &mcs
}

func (mcs *MultiConfigurationSource) GetDestinationConfigs() []*DestinationConfig {
	results := make([]*DestinationConfig, 0)
	for _, cs := range mcs.configurationSources {
		results = append(results, cs.GetDestinationConfigs()...)
	}
	return results
}

func (mcs *MultiConfigurationSource) GetDestinationConfig(id string) *DestinationConfig {
	for _, cs := range mcs.configurationSources {
		cfg := cs.GetDestinationConfig(id)
		if cfg != nil {
			return cfg
		}
	}
	return nil
}

func (mcs *MultiConfigurationSource) ChangesChannel() <-chan bool {
	return mcs.changesChan
}

func (mcs *MultiConfigurationSource) Close() error {
	close(mcs.closeChan)
	for _, cs := range mcs.configurationSources {
		_ = cs.Close()
	}
	close(mcs.changesChan)
	return nil

}
