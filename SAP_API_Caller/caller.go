package sap_api_caller

import (
	"fmt"
	"io/ioutil"
	"net/http"
	sap_api_output_formatter "sap-api-integrations-contract-reads-rmq-kube/SAP_API_Output_Formatter"
	"strings"
	"sync"

	"github.com/latonaio/golang-logging-library-for-sap/logger"
	"golang.org/x/xerrors"
)

type RMQOutputter interface {
	Send(sendQueue string, payload map[string]interface{}) error
}

type SAPAPICaller struct {
	baseURL      string
	apiKey       string
	outputQueues []string
	outputter    RMQOutputter
	log          *logger.Logger
}

func NewSAPAPICaller(baseUrl string, outputQueueTo []string, outputter RMQOutputter, l *logger.Logger) *SAPAPICaller {
	return &SAPAPICaller{
		baseURL:      baseUrl,
		apiKey:       GetApiKey(),
		outputQueues: outputQueueTo,
		outputter:    outputter,
		log:          l,
	}
}

func (c *SAPAPICaller) AsyncGetContract(iD, itemID, name string, accepter []string) {
	wg := &sync.WaitGroup{}
	wg.Add(len(accepter))
	for _, fn := range accepter {
		switch fn {
		case "ContractCollection":
			func() {
				c.ContractCollection(iD)
				wg.Done()
			}()
		case "ContractItemCollection":
			func() {
				c.ContractItemCollection(itemID)
				wg.Done()
			}()
		case "ContractName":
			func() {
				c.ContractName(name)
				wg.Done()
			}()
		default:
			wg.Done()
		}
	}

	wg.Wait()
}

func (c *SAPAPICaller) ContractCollection(iD string) {
	contractCollectionData, err := c.callContractSrvAPIRequirementContractCollection("ContractCollectionData", iD)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": contractCollectionData, "function": "ContractCollectionData"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(contractCollectionData)

	contractExternalPriceComponentData, err := c.callToContractExternalPriceComponent(contractCollectionData[0].ToContractExternalPriceComponent)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": contractExternalPriceComponentData, "function": "ContractExternalPriceComponentData"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(contractExternalPriceComponentData)

	contractItemData, err := c.callToContractItem(contractCollectionData[0].ToContractItem)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": contractItemData, "function": "ContractItemData"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(contractItemData)

	contractPartyData, err := c.callToContractParty(contractCollectionData[0].ToContractParty)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": contractPartyData, "function": "ContractPartyData"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(contractPartyData)
}

func (c *SAPAPICaller) callContractSrvAPIRequirementContractCollection(api, iD string) ([]sap_api_output_formatter.ContractCollection, error) {
	url := strings.Join([]string{c.baseURL, "c4codataapi", api}, "/")
	req, _ := http.NewRequest("GET", url, nil)

	c.setHeaderAPIKeyAccept(req)
	c.getQueryWithContractCollection(req, iD)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToContractCollection(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) callToContractExternalPriceComponent(url string) ([]sap_api_output_formatter.ToContractExternalPriceComponent, error) {
	req, _ := http.NewRequest("GET", url, nil)
	c.setHeaderAPIKeyAccept(req)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToToContractExternalPriceComponent(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) callToContractItem(url string) ([]sap_api_output_formatter.ToContractItem, error) {
	req, _ := http.NewRequest("GET", url, nil)
	c.setHeaderAPIKeyAccept(req)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToToContractItem(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) callToContractParty(url string) ([]sap_api_output_formatter.ToContractParty, error) {
	req, _ := http.NewRequest("GET", url, nil)
	c.setHeaderAPIKeyAccept(req)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToToContractParty(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) ContractItemCollection(itemID string) {
	contractItemCollectionData, err := c.callContractSrvAPIRequirementContractItemCollection("ContractItemCollectionData", itemID)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": contractItemCollectionData, "function": "ContractItemCollectionData"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(contractItemCollectionData)

	contractItemExternalPriceComponentData, err := c.callToContractItemExternalPriceComponent(contractItemCollectionData[0].ToContractItemExternalPriceComponent)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": contractItemExternalPriceComponentData, "function": "ContractItemExternalPriceComponentData"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(contractItemExternalPriceComponentData)

}

func (c *SAPAPICaller) callContractSrvAPIRequirementContractItemCollection(api, itemID string) ([]sap_api_output_formatter.ContractItemCollection, error) {
	url := strings.Join([]string{c.baseURL, "c4codataapi", api}, "/")
	req, _ := http.NewRequest("GET", url, nil)

	c.setHeaderAPIKeyAccept(req)
	c.getQueryWithContractItemCollection(req, itemID)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToContractItemCollection(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) callToContractItemExternalPriceComponent(url string) ([]sap_api_output_formatter.ToContractItemExternalPriceComponent, error) {
	req, _ := http.NewRequest("GET", url, nil)
	c.setHeaderAPIKeyAccept(req)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToToContractItemExternalPriceComponent(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) ContractName(name string) {
	contractNameData, err := c.callContractSrvAPIRequirementContractName("ContractCollection", name)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": contractNameData, "function": "ContractNameData"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(contractNameData)

	contractExternalPriceComponentData, err := c.callToContractExternalPriceComponent(contractNameData[0].ToContractExternalPriceComponent)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": contractExternalPriceComponentData, "function": "ContractExternalPriceComponentData"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(contractExternalPriceComponentData)

	contractItemData, err := c.callToContractItem(contractNameData[0].ToContractItem)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": contractItemData, "function": "ContractItemData"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(contractItemData)

	contractPartyData, err := c.callToContractParty(contractNameData[0].ToContractParty)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": contractPartyData, "function": "ContractPartyData"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(contractPartyData)
}

func (c *SAPAPICaller) callContractSrvAPIRequirementContractName(api, name string) ([]sap_api_output_formatter.ContractCollection, error) {
	url := strings.Join([]string{c.baseURL, "c4codataapi", api}, "/")
	req, _ := http.NewRequest("GET", url, nil)

	c.setHeaderAPIKeyAccept(req)
	c.getQueryWithContractName(req, name)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToContractCollection(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) setHeaderAPIKeyAccept(req *http.Request) {
	req.Header.Set("APIKey", c.apiKey)
	req.Header.Set("Accept", "application/json")
}

func (c *SAPAPICaller) getQueryWithContractCollection(req *http.Request, iD string) {
	params := req.URL.Query()
	params.Add("$filter", fmt.Sprintf("ID eq '%s'", iD))
	req.URL.RawQuery = params.Encode()
}

func (c *SAPAPICaller) getQueryWithContractItemCollection(req *http.Request, itemID string) {
	params := req.URL.Query()
	params.Add("$filter", fmt.Sprintf("ID eq '%s'", itemID))
	req.URL.RawQuery = params.Encode()
}

func (c *SAPAPICaller) getQueryWithContractName(req *http.Request, name string) {
	params := req.URL.Query()
	params.Add("$filter", fmt.Sprintf("substringof('%s', Name)", name))
	req.URL.RawQuery = params.Encode()
}
