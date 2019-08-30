/*
 * Copyright (C) 2019. Genome Research Ltd. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License,
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @file dublincore.go
 * @author Keith James <kdj@sanger.ac.uk>
 */

package dublincore

// https://www.dublincore.org/resources/userguide/publishing_metadata/
const (
	Namespace = "dcterms" // http://purl.org/dc/terms/

	Contributor = "dcterms:contributor" // http://purl.org/dc/elements/1.1/contributor
	Coverage    = "dcterms:coverage"    // http://purl.org/dc/elements/1.1/coverage
	Created     = "dcterms:created"     // http://purl.org/dc/elements/1.1/created
	Creator     = "dcterms:creator"     // http://purl.org/dc/elements/1.1/creator
	Date        = "dcterms:date"        // http://purl.org/dc/elements/1.1/date
	Description = "dcterms:description" // http://purl.org/dc/elements/1.1/description
	Format      = "dcterms:format"      // http://purl.org/dc/elements/1.1/format
	Identifier  = "dcterms:identifier"  // http://purl.org/dc/elements/1.1/identifier
	Language    = "dcterms:language"    // http://purl.org/dc/elements/1.1/language
	Modified    = "dcterms:modified"    // http://purl.org/dc/elements/1.1/modified
	Publisher   = "dcterms:publisher"   // http://purl.org/dc/elements/1.1/publisher
	Relation    = "dcterms:relation"    // http://purl.org/dc/elements/1.1/relation
	Rights      = "dcterms:rights"      // http://purl.org/dc/elements/1.1/rights
	Source      = "dcterms:source"      // http://purl.org/dc/elements/1.1/source
	Subject     = "dcterms:subject"     // http://purl.org/dc/elements/1.1/subject
	Title       = "dcterms:title"       // http://purl.org/dc/elements/1.1/title
	Type        = "dcterms:type"        // http://purl.org/dc/elements/1.1/type
)
