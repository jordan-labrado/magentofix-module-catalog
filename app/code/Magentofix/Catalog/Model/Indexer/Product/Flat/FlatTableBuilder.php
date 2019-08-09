<?php
/**
 * Copyright © Magento, Inc. All rights reserved.
 * See COPYING.txt for license details.
 */
namespace Magentofix\Catalog\Model\Indexer\Product\Flat;

use Magento\Catalog\Api\Data\ProductInterface;

/**
 * Class FlatTableBuilder
 * @SuppressWarnings(PHPMD.CouplingBetweenObjects)
 */
class FlatTableBuilder extends \Magento\Catalog\Model\Indexer\Product\Flat\FlatTableBuilder
{
    /**
     * Apply diff. between 0 store and current store to temporary flat table
     *
     * @param array $tables
     * @param array $changedIds
     * @param int|string $storeId
     * @param string $valueFieldSuffix
     * @return void
     */
    protected function _updateTemporaryTableByStoreValues(
        array $tables,
        array $changedIds,
        $storeId,
        $valueFieldSuffix
    ) {
        $flatColumns = $this->_productIndexerHelper->getFlatColumns();
        $temporaryFlatTableName = $this->_getTemporaryTableName(
            $this->_productIndexerHelper->getFlatTableName($storeId)
        );
        $linkField = $this->getMetadataPool()->getMetadata(ProductInterface::class)->getLinkField();
        foreach ($tables as $tableName => $columns) {
            foreach ($columns as $attribute) {
                /* @var $attribute \Magento\Eav\Model\Entity\Attribute */
                $attributeCode = $attribute->getAttributeCode();
                if ($attribute->getBackend()->getType() != 'static') {
                    $joinCondition = sprintf('t.%s = e.%s', $linkField, $linkField) .
                        ' AND t.attribute_id=' .
                        $attribute->getId() .
                        ' AND t.store_id = ' .
                        $storeId .
                        ' AND t.value IS NOT NULL';
                    /** @var $select \Magento\Framework\DB\Select */
                    $select = $this->_connection->select()
                        ->joinInner(
                            ['e' => $this->resource->getTableName('catalog_product_entity')],
                            'e.entity_id = et.entity_id',
                            []
                        )->joinInner(
                            ['t' => $tableName],
                            $joinCondition,
                            [$attributeCode => 't.value']
                        );
                    if (!empty($changedIds)) {
                        $select->where($this->_connection->quoteInto('et.entity_id IN (?)', $changedIds));
                    }

                    /*
                     * According to \Magento\Framework\DB\SelectRendererInterface select rendering may be updated
                     * so we need to trigger select renderer for correct update
                     */
                    $select->assemble();
                    $sql = $select->crossUpdateFromSelect(['et' => $temporaryFlatTableName]);
                    $this->_connection->query($sql);
                }

                //Update not simple attributes (eg. dropdown)
                $columnName = $attributeCode . $valueFieldSuffix;
                if (isset($flatColumns[$columnName])) {
                    $columnValue = $this->_connection->getIfNullSql('ts.value', 't0.value');
                    $select = $this->_connection->select();
                    $select->joinLeft(
                        ['t0' => $this->_productIndexerHelper->getTable('eav_attribute_option_value')],
                        't0.option_id = et.' . $attributeCode . ' AND t0.store_id = 0',
                        []
                    )->joinLeft(
                        ['ts' => $this->_productIndexerHelper->getTable('eav_attribute_option_value')],
                        'ts.option_id = et.' . $attributeCode . ' AND ts.store_id = ' . $storeId,
                        []
                    )->columns(
                        [$columnName => $columnValue]
                    // Fix for attribute codes corresponding to MySQL/MariaDB reserved keywords
                    )->where($columnValue . ' IS NOT NULL');
                    // )->where('et.' . $attributeCode . ' IS NOT NULL');
                    if (!empty($changedIds)) {
                        $select->where($this->_connection->quoteInto('et.entity_id IN (?)', $changedIds));
                    }
                    $select->assemble();
                    $sql = $select->crossUpdateFromSelect(['et' => $temporaryFlatTableName]);
                    $this->_connection->query($sql);
                }
            }
        }
    }

    /**
     * Get MetadataPool
     *
     * @return \Magento\Framework\EntityManager\MetadataPool
     */
    private function getMetadataPool()
    {
        if (null === $this->metadataPool) {
            $this->metadataPool = \Magento\Framework\App\ObjectManager::getInstance()
                ->get(\Magento\Framework\EntityManager\MetadataPool::class);
        }
        return $this->metadataPool;
    }
}
